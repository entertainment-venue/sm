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

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type shardServer struct {
	container *serverContainer

	lg *zap.Logger
}

type addSpecRequest struct {
	// 目前app的spec更多承担的是管理职能，shard配置的一个起点，先只配置上service，可以唯一标记一个app
	Service string `json:"service" binding:"required"`

	CreateTime int64 `json:"createTime" binding:"required"`
}

func (s *addSpecRequest) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (ss *shardServer) GinAddSpec(c *gin.Context) {
	var req addSpecRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info("receive add spec request", zap.String("request", req.String()))

	//  写入app spec和app task节点在一个tx
	var (
		nodes  []string
		values []string
	)
	nodes = append(nodes, nodeAppSpec(req.Service))
	nodes = append(nodes, apputil.EtcdPathAppShardTask(req.Service))
	values = append(values, req.String())
	values = append(values, "")
	if err := ss.container.Client.CreateAndGet(context.Background(), nodes, values, clientv3.NoLease); err != nil {
		ss.lg.Error("CreateAndGet err",
			zap.Error(err),
			zap.Strings("nodes", nodes),
			zap.Strings("values", values),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

type addShardRequest struct {
	ShardId string `json:"shardId" binding:"required"`

	// 为哪个业务app增加shard
	Service string `json:"service" binding:"required"`

	// 业务app自己定义task内容
	Task string `json:"task" binding:"required"`
}

func (r *addShardRequest) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

func (ss *shardServer) GinAddShard(c *gin.Context) {
	var req addShardRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info("receive add shard request", zap.String("request", req.String()))

	spec := apputil.ShardSpec{
		Service:    req.Service,
		Task:       req.Task,
		UpdateTime: time.Now().Unix(),
	}

	// 区分更新和添加
	// 如果是添加，等待负责该app的shard做探测即可
	// 如果是更新，shard是不允许更新的，这种更新的相当于shard工作内容的调整
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

func (ss *shardServer) GinAppDelShard(c *gin.Context) {
	var req delShardRequest
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ss.lg.Info("receive del shard request", zap.String("request", req.String()))

	// 获取shard当前的基本信息
	key := apputil.EtcdPathAppShardId(req.Service, req.ShardId)
	resp, err := ss.container.Client.Get(context.Background(), key, nil)
	if err != nil {
		ss.lg.Error("failed to get etcd node",
			zap.Error(err),
			zap.String("key", key),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if resp.Count == 0 {
		ss.lg.Error("etcd node not exist", zap.String("key", key))
		c.JSON(http.StatusInternalServerError, gin.H{"error": errNotExist})
		return
	}

	var spec apputil.ShardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &spec); err != nil {
		ss.lg.Error("Unmarshal err",
			zap.Error(err),
			zap.ByteString("value", resp.Kvs[0].Value),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 要求特定服务drop某个shard，http请求
	ma := moveAction{
		Service:      req.Service,
		ShardId:      req.ShardId,
		DropEndpoint: spec.ContainerId,
	}
	if err := ss.container.op.dropOrAdd(context.TODO(), &ma); err != nil {
		ss.lg.Error("failed to dropOrAdd",
			zap.Error(err),
			zap.Reflect("ma", ma),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ss.lg.Info("delete shard success", zap.Reflect("req", req))
	c.JSON(http.StatusOK, gin.H{})
}
