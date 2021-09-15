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

package apputil

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ShardSpec struct {
	// 存储下，方便开发
	Service string `json:"service"`

	// 任务内容
	Task string `json:"task"`

	UpdateTime int64 `json:"updateTime"`

	// 所属container，在任务分配场景设置
	ContainerId string `json:"containerId"`

	// TODO 提供后台移动shard的能力，但是该能力开发较复杂，暂时不提供
	// 软删除：borderland的shard会检查shard是否都分配给了健康的container，已经存在goroutine，直接在这个goroutine中处理删除
	// Deleted bool `json:"deleted"`
}

func (ss *ShardSpec) String() string {
	b, _ := json.Marshal(ss)
	return string(b)
}

type ShardInterface interface {
	Add(ctx context.Context, id string, spec *ShardSpec) error
	Drop(ctx context.Context, id string) error
	Load(ctx context.Context, id string) (string, error)
}

type ShardOpMessage struct {
	Id   string `json:"id"`
	Type OpType `json:"type"`
}

type OpType int

const (
	// 0保留，因为go默认int的值是0，防止无意识的错误
	// 参考：https://github.com/etcd-io/etcd/blob/main/client/v3/op.go#L22
	OpTypeAdd OpType = iota + 1
	OpTypeDrop
)

// 直接帮助接入方把服务器端启动好，引入gin框架，和sarama sdk的接入方式相似，提供消息的chan或者callback func给到接入app的业务逻辑
type ShardServer struct {
	stopper  *GoroutineStopper
	shards   map[string]struct{}
	hbNode   string
	taskNode string

	impl      ShardInterface
	container *Container
	lg        *zap.Logger
}

type shardServerOptions struct {
	ctx             context.Context
	addr            string
	routeAndHandler map[string]func(c *gin.Context)

	// FIXME 和ShardServer重复
	implementation ShardInterface
	container      *Container
	lg             *zap.Logger
}

type ShardServerOption func(options *shardServerOptions)

func ShardServerWithAddr(v string) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.addr = v
	}
}

func ShardServerWithContainer(v *Container) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.container = v
	}
}

func ShardServerWithShardImplementation(v ShardInterface) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.implementation = v
	}
}

func ShardServerWithContext(v context.Context) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.ctx = v
	}
}

func ShardServerWithApiHandler(routeAndHandler map[string]func(c *gin.Context)) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.routeAndHandler = routeAndHandler
	}
}

func ShardServerWithLogger(lg *zap.Logger) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.lg = lg
	}
}

func NewShardServer(opts ...ShardServerOption) error {
	ops := &shardServerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	if ops.addr == "" {
		return errors.New("addr err")
	}

	logger, _ := zap.NewProduction()

	ss := ShardServer{
		shards:   make(map[string]struct{}),
		stopper:  &GoroutineStopper{},
		hbNode:   EtcdPathAppShardHbId(ops.container.Service(), ops.container.Id()),
		taskNode: EtcdPathAppShardId(ops.container.Service(), ops.container.Id()),
		lg:       logger,
	}

	r := gin.Default()
	ssg := r.Group("/sm/admin")
	{
		ssg.POST("/add-shard", ss.GinAddShard)
		ssg.POST("/drop-shard", ss.GinDropShard)
	}
	for route, handler := range ops.routeAndHandler {
		r.POST(route, handler)
	}

	if err := r.Run(ops.addr); err != nil {
		return errors.Wrap(err, "")
	}

	// 上传shard的load，load是从接入服务拿到
	ss.stopper.Wrap(func(ctx context.Context) {
		TickerLoop(
			ctx, logger, 3*time.Second, "[shardserver] load uploader exit",
			func(ctx context.Context) error {
				for id := range ss.shards {
					sl, err := ss.impl.Load(context.TODO(), id)
					if err != nil {
						return errors.Wrap(err, "")
					}

					if _, err := ss.container.Client.Put(ctx, ss.hbNode, sl, clientv3.WithLease(ss.container.Session.Lease())); err != nil {
						return errors.Wrap(err, "")
					}
				}
				return nil
			},
		)
	})

	go func() {
		defer ss.Close()
		for range ops.ctx.Done() {

		}
	}()

	return nil
}

func (ss *ShardServer) Close() {
	if ss.stopper != nil {
		ss.stopper.Close()
	}
}

func (ss *ShardServer) GinAddShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeAdd
	ss.lg.Info("add shard request", zap.Reflect("request", req))

	resp, err := ss.container.Client.GetKV(context.TODO(), ss.taskNode, nil)
	if err != nil {
		ss.lg.Error("GetKV err",
			zap.Error(err),
			zap.String("taskNode", ss.taskNode),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if resp.Count == 0 {
		err = errors.Errorf("Failed to get shard %s content", req.Id)
		ss.lg.Error("no shard exist",
			zap.String("id", req.Id),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var spec ShardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &spec); err != nil {
		ss.lg.Error("Unmarshal err",
			zap.Error(err),
			zap.ByteString("value", resp.Kvs[0].Value),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := ss.impl.Add(context.TODO(), req.Id, &spec); err != nil {
		ss.lg.Error("Add err",
			zap.Error(err),
			zap.String("id", req.Id),
			zap.Reflect("spec", spec),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

func (ss *ShardServer) GinDropShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		ss.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeDrop

	if err := ss.impl.Drop(context.TODO(), req.Id); err != nil {
		ss.lg.Error("Drop err",
			zap.Error(err),
			zap.String("id", req.Id),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ss.lg.Info("drop shard success", zap.Reflect("req", req))
	c.JSON(http.StatusOK, gin.H{})
}
