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

	// 停掉worker
	shard, err := ss.container.GetShard(service)
	if err != nil {
		err := errors.Errorf("param error")
		ss.lg.Error(
			"shard not found",
			zap.String("service", service),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	shard.Close()

	// 清除etcd数据
	pfx := ss.container.nodeManager.nodeServiceShard(ss.container.Service(), service)
	if err := ss.container.Client.DelKV(context.Background(), pfx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info(
		"delete spec success",
		zap.String("pfx", pfx),
	)
	c.JSON(http.StatusOK, gin.H{})
}

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

// @Description update spec
// @Tags  spec
// @Accept  json
// @Produce  json
// @Param param body smAppSpec true "param"
// @success 200
// @Router /sm/server/update-spec [post]
func (ss *smShardApi) GinUpdateSpec(c *gin.Context) {
	var req smAppSpec
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.CreateTime = time.Now().Unix()
	ss.lg.Info("receive update spec request", zap.String("request", req.String()))
	//  查询是否存在该service
	shard, err := ss.container.GetShard(req.Service)
	if err != nil {
		ss.lg.Error(
			"update service err",
			zap.String("service", req.Service),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "service not exist"})
		return
	}

	pfx := ss.container.nodeManager.nodeServiceSpec(req.Service)
	if err := ss.container.Client.UpdateKV(context.Background(), pfx, req.String()); err != nil {
		ss.lg.Error("UpdateKV err",
			zap.String("pfx", pfx),
			zap.String("value", req.String()),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	//  更新sm container内存中的值
	shard.SetMaxShardCount(req.MaxShardCount)
	shard.SetMaxRecoveryTime(req.MaxRecoveryTime)

	ss.lg.Info("update spec success", zap.String("pfx", pfx))
	c.JSON(http.StatusOK, gin.H{})
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
}

func (r *addShardRequest) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

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
