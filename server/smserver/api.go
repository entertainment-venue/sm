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
	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type shardServer struct {
	container *smContainer

	lg *zap.Logger
}

type smAppSpec struct {
	// Service 目前app的spec更多承担的是管理职能，shard配置的一个起点，先只配置上service，可以唯一标记一个app
	Service string `json:"service" binding:"required"`

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

// @Description add spec
// @Tags  spec
// @Accept  json
// @Produce  json
// @Param param body smAppSpec true "param"
// @success 200
// @Router /sm/server/add-spec [post]
func (ss *shardServer) GinAddSpec(c *gin.Context) {
	var req smAppSpec
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.CreateTime = time.Now().Unix()
	ss.lg.Info("receive add spec request", zap.String("request", req.String()))

	//  写入app spec和app task节点在一个tx
	var (
		nodes  []string
		values []string
	)

	nodes = append(nodes, nodeAppSpec(req.Service))
	values = append(values, req.String())

	// 如果不是sm自己的spec,那么需要将service注册到sm的spec中
	if ss.container.service != req.Service {
		v := apputil.ShardSpec{
			Service:    ss.container.service,
			Task:       fmt.Sprintf("{\"governedService\":\"%s\"}", req.Service),
			UpdateTime: time.Now().Unix(),
		}
		nodes = append(nodes, apputil.EtcdPathAppShardId(ss.container.service, req.Service))
		values = append(values, v.String())
	}
	if err := ss.container.Client.CreateAndGet(context.Background(), nodes, values, clientv3.NoLease); err != nil {
		ss.lg.Error("CreateAndGet err",
			zap.Strings("nodes", nodes),
			zap.Strings("values", values),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
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
func (ss *shardServer) GinDelSpec(c *gin.Context) {
	service := c.Query("service")
	if service == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "service empty"})
		return
	}
	// 软删除，将删除标记写到sm container的shard中，由sm来进行删除以及资源释放
	resp, err := ss.container.Client.Get(context.Background(), apputil.EtcdPathAppShardId(ss.container.service, service))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(resp.Kvs) != 1 {
		ss.lg.Warn("spec not exist",
			zap.String("service", service),
		)
		c.JSON(http.StatusOK, gin.H{})
		return
	}
	var newV apputil.ShardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &newV); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	newV.Type = specDelType

	if err := ss.container.Client.UpdateKV(context.Background(), apputil.EtcdPathAppShardId(ss.container.service, service), newV.String()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info("delete spec success", zap.String("service", service))
	c.JSON(http.StatusOK, gin.H{})
}

// @Description get all service
// @Tags  spec
// @Accept  json
// @Produce  json
// @success 200
// @Router /sm/server/get-spec [get]
func (ss *shardServer) GinGetSpec(c *gin.Context) {
	kvs, err := ss.container.Client.GetKVs(context.Background(), apputil.EtcdPathAppShardId(ss.container.service, ""))
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
func (ss *shardServer) GinUpdateSpec(c *gin.Context) {
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
		ss.lg.Error("update service err", zap.String("service", req.Service), zap.String("err", err.Error()))
		c.JSON(http.StatusBadRequest, gin.H{"error": "service not exist"})
		return
	}
	if err := ss.container.Client.UpdateKV(context.Background(), nodeAppSpec(req.Service), req.String()); err != nil {
		ss.lg.Error("UpdateKV err",
			zap.String("key", nodeAppSpec(req.Service)),
			zap.String("value", req.String()),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	//  更新sm container内存中的值
	shard.worker.SetMaxShardCount(req.MaxShardCount)
	shard.worker.SetMaxRecoveryTime(req.MaxRecoveryTime)

	ss.lg.Info("update spec success", zap.String("service", req.Service))
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
func (ss *shardServer) GinAddShard(c *gin.Context) {
	var req addShardRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info("receive add shard request", zap.String("request", req.String()))

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
		nodes  = []string{apputil.EtcdPathAppShardId(req.Service, req.ShardId)}
		values = []string{spec.String()}
	)
	if err := ss.container.Client.CreateAndGet(context.Background(), nodes, values, clientv3.NoLease); err != nil {
		ss.lg.Error("failed to add shard",
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
func (ss *shardServer) GinDelShard(c *gin.Context) {
	var req delShardRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info("receive del shard request", zap.String("request", req.String()))

	ctx := context.Background()

	// 删除shard节点
	shardNode := apputil.EtcdPathAppShardId(req.Service, req.ShardId)
	delResp, err := ss.container.Client.Delete(ctx, shardNode)
	if err != nil {
		ss.lg.Error("delete err",
			zap.Error(err),
			zap.String("shardNode", shardNode),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if delResp.Deleted != 1 {
		ss.lg.Warn("shard not exist",
			zap.Reflect("req", req),
			zap.String("shardNode", shardNode),
		)
		c.JSON(http.StatusOK, gin.H{})
		return
	}

	ss.lg.Info("delete shard success", zap.Reflect("req", req))
	c.JSON(http.StatusOK, gin.H{})
}

// @Description get service all shard
// @Tags  shard
// @Accept  json
// @Produce  json
// @Param service query string true "param"
// @success 200
// @Router /sm/server/get-shard [get]
func (ss *shardServer) GinGetShard(c *gin.Context) {
	service := c.Query("service")
	if service == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "service empty"})
		return
	}
	kvs, err := ss.container.Client.GetKVs(context.Background(), apputil.EtcdPathAppShardId(service, ""))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var shards []string
	for s, _ := range kvs {
		shards = append(shards, s)
	}
	ss.lg.Info("get shards success", zap.String("service", service))
	c.JSON(http.StatusOK, gin.H{"shards": shards})
}
