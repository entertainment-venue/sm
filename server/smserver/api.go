// Copyright 2021 The entertainment-venue Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package smserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type smAppSpec struct {
	// Service 目前app的spec更多承担的是管理职能，shard配置的一个起点，先只配置上service，可以唯一标记一个app
	Service string `json:"service"`

	CreateTime int64 `json:"createTime"`

	// MaxShardCount 单container承载的最大分片数量，防止雪崩
	MaxShardCount int `json:"maxShardCount"`

	// MaxRecoveryTime 遇到container删除的场景，等待的时间，超时认为该container被清理
	MaxRecoveryTime int `json:"maxRecoveryTime"`
}

func (s *smAppSpec) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type smShardApi struct {
	container *smContainer

	lg *zap.Logger
}

func newSMShardApi(container *smContainer) *smShardApi {
	return &smShardApi{container: container, lg: container.lg}
}

// GinAddSpec
// @Description add spec
// @Tags  spec
// @Accept  json
// @Produce  json
// @Param param body smAppSpec true "param"
// @success 200
// @Router /sm/server/add-spec [post]
func (ss *smShardApi) GinAddSpec(c *gin.Context) {
	var req smAppSpec
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.CreateTime = time.Now().Unix()
	ss.lg.Info("receive add spec request", zap.Reflect("request", req))

	// sm的service是保留service，在程序启动的时候初始化
	if req.Service == ss.container.Service() {
		err := errors.Errorf("Same as shard manager's service")
		ss.lg.Error("service error", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	//  写入app spec和app task节点在一个tx
	var (
		nodes  []string
		values []string
	)

	// 业务节点的service放在sm的pfx下面
	nodes = append(nodes, ss.container.nodeManager.nodeServiceSpec(req.Service))
	values = append(values, req.String())

	// 创建guard lease节点
	nodes = append(nodes, ss.container.nodeManager.nodeServiceGuard(req.Service))
	lease := apputil.Lease{}
	values = append(values, lease.String())

	// 创建containerhb节点
	nodes = append(nodes, ss.container.nodeManager.nodeServiceContainerHb(req.Service))
	values = append(values, "")

	// 需要将service注册到sm的spec中
	t := shardTask{GovernedService: req.Service}
	v := apputil.ShardSpec{
		Service:    ss.container.Service(),
		Task:       t.String(),
		UpdateTime: time.Now().Unix(),
	}
	nodes = append(nodes, ss.container.nodeManager.nodeServiceShard(ss.container.Service(), req.Service))
	values = append(values, v.String())
	if err := ss.container.Client.CreateAndGet(context.Background(), nodes, values, clientv3.NoLease); err != nil {
		if err != etcdutil.ErrEtcdNodeExist {
			ss.lg.Error("CreateAndGet err",
				zap.Strings("nodes", nodes),
				zap.Strings("values", values),
				zap.Error(err),
			)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		ss.lg.Warn("CreateAndGet node exist",
			zap.Strings("nodes", nodes),
			zap.Strings("values", values),
			zap.Error(err),
		)
	}
	ss.lg.Info("add spec success", zap.String("service", req.Service))
	c.JSON(http.StatusOK, gin.H{})
}

// GinDelSpec
// @Description del spec
// @Tags  spec
// @Accept  json
// @Produce  json
// @Param service query string true "param"
// @success 200
// @Router /sm/server/del-spec [get]
func (ss *smShardApi) GinDelSpec(c *gin.Context) {

	// 策略是停掉worker、删除etcd中的分片，service自己停掉服务即可
	// 如果关注service正在运行，设计过于复杂，service中的shard如果部分存活状态，很难做到graceful，需要人工介入

	service := c.Query("service")
	if service == "" {
		err := errors.Errorf("param error")
		ss.lg.Error(
			"empty service",
			zap.String("service", service),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// 不允许删除sm
	if service == ss.container.Service() {
		err := errors.Errorf("param error")
		ss.lg.Error(
			"same as shard manager's service",
			zap.String("service", service),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 清除etcd数据
	var nodes []string
	nodes = append(nodes, ss.container.nodeManager.nodeService(service))
	nodes = append(nodes, ss.container.nodeManager.nodeServiceSpec(service))
	nodes = append(nodes, ss.container.nodeManager.nodeServiceShard(ss.container.Service(), service))
	if err := ss.container.Client.DelKVs(context.Background(), nodes); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info(
		"delete spec success",
		zap.Strings("nodes", nodes),
	)
	c.JSON(http.StatusOK, gin.H{})
}

// GinGetSpec
// @Description get all service
// @Tags  spec
// @Accept  json
// @Produce  json
// @success 200
// @Router /sm/server/get-spec [get]
func (ss *smShardApi) GinGetSpec(c *gin.Context) {
	pfx := ss.container.nodeManager.nodeServiceShard(ss.container.Service(), "")
	kvs, err := ss.container.Client.GetKVs(context.Background(), pfx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var services []string
	for s, _ := range kvs {
		services = append(services, s)
	}
	ss.lg.Info("get all service success")
	c.JSON(http.StatusOK, gin.H{"services": services})
}

type addShardRequest struct {
	ShardId string `json:"shardId" binding:"required"`

	// 为哪个业务app增加shard
	Service string `json:"service" binding:"required"`

	// 业务app自己定义task内容
	Task string `json:"task"`

	ManualContainerId string `json:"manualContainerId"`

	// Group 同一个service需要区分不同种类的shard，这些shard之间不相关的balance到现有container上
	Group string `json:"group"`

	// WorkerGroup 同一个service需要区分不同种类的container，shard可以指定分配到那一组container上
	WorkerGroup string `json:"workerGroup"`
}

func (r *addShardRequest) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// GinAddShard
// @Description add shard
// @Tags  shard
// @Accept  json
// @Produce  json
// @Param param body addShardRequest true "param"
// @success 200
// @Router /sm/server/add-shard [post]
func (ss *smShardApi) GinAddShard(c *gin.Context) {
	var req addShardRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info(
		"add shard request",
		zap.Reflect("req", req),
	)

	// sm本身的shard是和service添加绑定的，不需要走这个接口
	if req.Service == ss.container.Service() {
		err := errors.Errorf("same as shard manager's service")
		ss.lg.Error("service error", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 检查是否存在该service
	resp, err := ss.container.Client.GetKV(context.Background(), ss.container.nodeManager.nodeServiceSpec(req.Service), nil)
	if err != nil {
		ss.lg.Error("GetKV error",
			zap.Error(err),
			zap.String("service node", ss.container.nodeManager.nodeServiceSpec(req.Service)),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if resp.Count == 0 {
		err := errors.Errorf(fmt.Sprintf("service[%s] not exist", req.Service))
		ss.lg.Error("service error", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	spec := apputil.ShardSpec{
		Service:           req.Service,
		Task:              req.Task,
		UpdateTime:        time.Now().Unix(),
		ManualContainerId: req.ManualContainerId,
		Group:             req.Group,
	}

	// 区分更新和添加
	// 添加: 等待负责该app的shard做探测即可
	// 更新: shard是不允许更新的，这种更新的相当于shard工作内容的调整
	var (
		nodes  = []string{ss.container.nodeManager.nodeServiceShard(req.Service, req.ShardId)}
		values = []string{spec.String()}
	)
	if err := ss.container.Client.CreateAndGet(context.Background(), nodes, values, clientv3.NoLease); err != nil {
		ss.lg.Error("CreateAndGet error",
			zap.Error(err),
			zap.Strings("nodes", nodes),
			zap.Strings("values", values),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

type delShardRequest struct {
	ShardId string `json:"shardId" binding:"required"`
	Service string `json:"service" binding:"required"`
}

func (r *delShardRequest) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

// GinDelShard
// @Description del shard
// @Tags  shard
// @Accept  json
// @Produce  json
// @Param param body delShardRequest true "param"
// @success 200
// @Router /sm/server/del-shard [post]
// GinDelShard TODO ACL 需要带着key过来做分片的移动，防止跨租户之间有影响
func (ss *smShardApi) GinDelShard(c *gin.Context) {
	var req delShardRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info("del shard request", zap.Reflect("req", req))

	// 删除shard节点
	pfx := ss.container.nodeManager.nodeServiceShard(req.Service, req.ShardId)
	delResp, err := ss.container.Client.Delete(context.TODO(), pfx)
	if err != nil {
		ss.lg.Error("Delete err",
			zap.Error(err),
			zap.String("pfx", pfx),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if delResp.Deleted != 1 {
		ss.lg.Warn("shard not exist",
			zap.Reflect("req", req),
			zap.String("pfx", pfx),
		)
		c.JSON(http.StatusOK, gin.H{})
		return
	}

	ss.lg.Info(
		"delete shard success",
		zap.Reflect("req", req),
		zap.String("pfx", pfx),
	)
	c.JSON(http.StatusOK, gin.H{})
}

// GinGetShard
// @Description get service all shard
// @Tags  shard
// @Accept  json
// @Produce  json
// @Param service query string true "param"
// @success 200
// @Router /sm/server/get-shard [get]
func (ss *smShardApi) GinGetShard(c *gin.Context) {
	service := c.Query("service")
	if service == "" {
		err := errors.Errorf("param error")
		ss.lg.Error(
			"empty service",
			zap.String("service", service),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	pfx := ss.container.nodeManager.nodeServiceShard(service, "")
	kvs, err := ss.container.Client.GetKVs(context.TODO(), pfx)
	if err != nil {
		ss.lg.Error(
			"GetKVs error",
			zap.String("service", service),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var shards []string
	for s, _ := range kvs {
		shards = append(shards, s)
	}
	ss.lg.Info(
		"get shards success",
		zap.String("pfx", pfx),
		zap.Strings("shards", shards),
	)
	c.JSON(http.StatusOK, gin.H{"shards": shards})
}

type workerRequest struct {
	// 在哪个资源组下面添加worker
	WorkerGroup string `json:"workerGroup" binding:"required"`

	// 为哪个业务app增加worker
	Service string `json:"service" binding:"required"`

	// 需要添加的资源，添加后，shard中如果存在WorkerGroup，只会将shard分配到该WorkerGroup下的worker中。
	Worker string `json:"worker" binding:"required"`
}

// GinAddWorker
// @Description add service worker
// @Tags  worker
// @Accept  json
// @Produce  json
// @Param param body workerRequest true "param"
// @success 200
// @Router /sm/server/add-worker [post]
func (ss *smShardApi) GinAddWorker(c *gin.Context) {
	var req workerRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info(
		"add worker request",
		zap.Reflect("req", req),
	)

	// 检查是否存在该service
	resp, err := ss.container.Client.GetKV(context.Background(), ss.container.nodeManager.nodeServiceSpec(req.Service), nil)
	if err != nil {
		ss.lg.Error("GetKV error",
			zap.String("service node", ss.container.nodeManager.nodeServiceSpec(req.Service)),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if resp.Count == 0 {
		ss.lg.Warn("service not exist", zap.String("service", req.Service))
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("service[%s] not exist", req.Service)})
		return
	}

	var (
		nodes  = []string{ss.container.nodeManager.nodeServiceWorker(req.Service, req.WorkerGroup, req.Worker)}
		values = []string{""}
	)
	if err := ss.container.Client.CreateAndGet(context.Background(), nodes, values, clientv3.NoLease); err != nil {
		ss.lg.Error("CreateAndGet error",
			zap.Strings("nodes", nodes),
			zap.Strings("values", values),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{})
}

// GinDelWorker
// @Description del service worker
// @Tags  worker
// @Accept  json
// @Produce  json
// @Param param body workerRequest true "param"
// @success 200
// @Router /sm/server/del-worker [post]
func (ss *smShardApi) GinDelWorker(c *gin.Context) {
	var req workerRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info(
		"del worker request",
		zap.Reflect("req", req),
	)

	// 删除worker节点
	pfx := ss.container.nodeManager.nodeServiceWorker(req.Service, req.WorkerGroup, req.Worker)
	delResp, err := ss.container.Client.Delete(context.TODO(), pfx)
	if err != nil {
		ss.lg.Error("Delete err",
			zap.String("pfx", pfx),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if delResp.Deleted != 1 {
		ss.lg.Warn("worker not exist",
			zap.Reflect("req", req),
			zap.String("pfx", pfx),
		)
		c.JSON(http.StatusOK, gin.H{})
		return
	}

	ss.lg.Info(
		"delete worker success",
		zap.Reflect("req", req),
		zap.String("pfx", pfx),
	)
	c.JSON(http.StatusOK, gin.H{})
}

// GinGetWorker
// @Description get service workerpool all worker
// @Tags  worker
// @Accept  json
// @Produce  json
// @Param service query string true "param"
// @success 200
// @Router /sm/server/get-worker [get]
func (ss *smShardApi) GinGetWorker(c *gin.Context) {
	service := c.Query("service")
	if service == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "service and workerGroup must not empty"})
		return
	}
	result := map[string][]string{}
	pfx := ss.container.nodeManager.nodeServiceWorkerGroup(service)
	resp, err := ss.container.Client.Get(context.TODO(), pfx, clientv3.WithPrefix())
	if err != nil {
		ss.lg.Error(
			"GetKVs error",
			zap.String("service", service),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	for _, kv := range resp.Kvs {
		// /sm/app/foo.bar/service/foo.bar/workerpool/g1/127.0.0.1:8801
		arr := strings.Split(string(kv.Key), "/")
		if len(arr)-2 > 0 {
			if _, ok := result[arr[len(arr)-2]]; ok {
				result[arr[len(arr)-2]] = append(result[arr[len(arr)-2]], arr[len(arr)-1])
				continue
			}
			result[arr[len(arr)-2]] = []string{arr[len(arr)-1]}
		}
	}
	ss.lg.Info(
		"get worker success",
		zap.String("pfx", pfx),
		zap.Reflect("shards", result),
	)
	c.JSON(http.StatusOK, gin.H{"workers": result})
}

func (ss *smShardApi) GinHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"msg": "success"})
}
