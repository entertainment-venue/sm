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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type ShardAction int

const (
	ShardActionDelete ShardAction = iota + 1
)

type ShardSpec struct {
	// 存储下，方便开发
	Service string `json:"service"`

	// 任务内容
	Task string `json:"task"`

	UpdateTime int64 `json:"updateTime"`

	// 通过api可以给shard主动分配到某个container
	ManualContainerId string `json:"manualContainerId"`

	// Group 同一个service需要区分不同种类的shard，
	// 这些shard之间不相关的balance到现有container上
	Group string `json:"group"`

	// ActionType 标记当前ShardSpec所处状态，smserver删除分片
	Action ShardAction `json:"action"`
}

func (ss *ShardSpec) String() string {
	b, _ := json.Marshal(ss)
	return string(b)
}

func (ss *ShardSpec) Validate() error {
	if ss.Service == "" {
		return errors.New("Empty service")
	}
	if ss.Task == "" {
		return errors.New("Empty task")
	}
	if ss.UpdateTime <= 0 {
		return errors.New("Err updateTime")
	}
	return nil
}

type ShardHeartbeat struct {
	Heartbeat

	Load        string `json:"load"`
	ContainerId string `json:"containerId"`
}

func (s *ShardHeartbeat) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type ShardInterface interface {
	Add(id string, spec *ShardSpec) error
	Drop(id string) error
	Load(id string) (string, error)
}

type ShardOpMessage struct {
	Id      string `json:"id"`
	Type    OpType `json:"type"`
	TraceId string `json:"traceId"`
}

type OpType int

const (
	// OpTypeAdd 0保留，因为go默认int的值是0，防止无意识的错误
	// 参考：https://github.com/etcd-io/etcd/blob/main/client/v3/op.go#L22
	OpTypeAdd OpType = iota + 1
	OpTypeDrop
)

type ShardOpReceiver interface {
	AddShard(c *gin.Context)
	DropShard(c *gin.Context)
}

// ShardServer 直接帮助接入方把服务器端启动好，引入gin框架，和sarama sdk的接入方式相似，提供消息的chan或者callback func给到接入app的业务逻辑
type ShardServer struct {
	stopper *GoroutineStopper

	// 在Close方法中需要能被close掉
	srv *http.Server

	donec chan struct{}

	// opts 存储选项中的数据，没必要copy一遍
	opts *shardServerOptions

	// keeper 代理shard的操作，封装bolt操作进去
	keeper *shardKeeper

	mu sync.Mutex
	// closed 导致 ShardServer 被关闭的事件是异步的，需要做保护
	closed bool
}

type shardServerOptions struct {
	addr            string
	routeAndHandler map[string]func(c *gin.Context)

	impl      ShardInterface
	container *Container
	lg        *zap.Logger
	sor       ShardOpReceiver

	// 传入 router 允许shard被集成，降低shard接入对app造成的影响。
	// 例如：现有的web项目使用gin，sm把server启动拿过来也不合适。
	router *gin.Engine

	// etcdPrefix 作为sharded application的数据存储prefix，能通过acl做限制
	// TODO 配合 etcdPrefix 需要有用户名和密码的字段
	etcdPrefix string
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
		sso.impl = v
	}
}

func ShardServerWithApiHandler(v map[string]func(c *gin.Context)) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.routeAndHandler = v
	}
}

func ShardServerWithLogger(v *zap.Logger) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.lg = v
	}
}

func ShardServerWithShardOpReceiver(v ShardOpReceiver) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.sor = v
	}
}

func ShardServerWithRouter(v *gin.Engine) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.router = v
	}
}

func ShardServerWithEtcdPrefix(v string) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.etcdPrefix = v
	}
}

func NewShardServer(opts ...ShardServerOption) (*ShardServer, error) {
	ops := &shardServerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	// addr 和 router 二选一，否则啥也不用干了
	if ops.addr == "" && ops.router == nil {
		return nil, errors.New("addr err")
	}
	if ops.container == nil {
		return nil, errors.New("container err")
	}
	if ops.lg == nil {
		return nil, errors.New("lg err")
	}
	if ops.impl == nil {
		return nil, errors.New("impl err")
	}

	// FIXME 直接刚常量有点粗糙，暂时没有更好的方案
	InitEtcdPrefix(ops.etcdPrefix)

	ss := ShardServer{
		stopper: &GoroutineStopper{},
		donec:   make(chan struct{}),
		opts:    ops,
	}

	// keeper: 向调用方下发shard move指令，提供本地持久存储能力
	keeper, err := newShardKeeper(ops.lg, ops.container.Service(), ops.impl)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	ss.keeper = keeper

	// heartbeat:
	ss.stopper.Wrap(func(ctx context.Context) {
		TickerLoop(
			ctx,
			ops.lg,
			3*time.Second,
			fmt.Sprintf("shardserver: service %s stop heartbeat", ss.opts.container.Service()),
			func(ctx context.Context) error {
				hbFn := func(k, v []byte) error {
					id := string(k)
					load, err := ss.keeper.Load(id)
					if err != nil {
						ops.lg.Error(
							"call Load error",
							zap.Reflect("id", id),
							zap.Error(err),
						)
						return nil
					}

					hb := ShardHeartbeat{
						Load:        load,
						ContainerId: ss.opts.container.Id(),
					}
					hb.Timestamp = time.Now().Unix()

					session := ss.opts.container.Session

					// lock: 失败场景打印日志，不影响其他shard的heartbeat
					lockPfx := EtcdPathAppShardHbId(ss.opts.container.Service(), id)
					mutex := concurrency.NewMutex(session, lockPfx)
					if err := mutex.Lock(ss.opts.container.Client.Client.Ctx()); err != nil {
						ops.lg.Error(
							"lock error",
							zap.String("pfx", lockPfx),
							zap.Error(err),
						)
						return nil
					}

					dataPfx := fmt.Sprintf("%s/%x", lockPfx, session.Lease())
					if _, err := ss.opts.container.Client.Put(ctx, dataPfx, hb.String(), clientv3.WithLease(session.Lease())); err != nil {
						ops.lg.Error(
							"put error",
							zap.String("pfx", dataPfx),
							zap.Reflect("hb", hb),
							zap.Error(err),
						)
						return nil
					}
					ops.lg.Debug("shard heartbeat", zap.String("hbNode", dataPfx))
					return nil
				}
				return errors.Wrap(ss.keeper.forEach(hbFn), "")
			},
		)
	})

	go func() {
		select {
		case <-ss.donec:
			// 被动关闭
			ss.opts.lg.Info(
				"shardserver: stopper closed",
				zap.String("service", ss.opts.container.Service()),
			)
		case <-ss.opts.container.Session.Done():
			ss.close()

			ss.opts.lg.Info(
				"shardserver: session closed",
				zap.String("service", ss.opts.container.Service()),
			)
		}
	}()

	router := ops.router
	if ops.router == nil {
		router = gin.Default()
		if ops.routeAndHandler != nil {
			for route, handler := range ops.routeAndHandler {
				router.Any(route, handler)
			}
		}
	}

	var receiver ShardOpReceiver
	if ops.sor != nil {
		receiver = ops.sor
	} else {
		receiver = &ss
	}
	// 是否需要跳过给router挂接口
	var skip bool
	routes := router.Routes()
	if routes != nil {
		for _, route := range routes {
			if strings.HasPrefix(route.Path, "/sm/admin") {
				skip = true
				break
			}
		}
	}
	if !skip {
		ssg := router.Group("/sm/admin")
		{
			ssg.POST("/add-shard", receiver.AddShard)
			ssg.POST("/drop-shard", receiver.DropShard)
		}
	}

	// router 为空，就帮助启动webserver，相当于app自己选择被集成，例如sm自己
	if ops.router == nil {
		// https://learnku.com/docs/gin-gonic/2019/examples-graceful-restart-or-stop/6173
		srv := &http.Server{
			Addr:    ops.addr,
			Handler: router,
		}
		ss.srv = srv

		// FIXME 这个goroutine在退出时，没有回收当前资源，后续，会改造把gin从sm剔除掉
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				ops.lg.Panic(
					"failed to listen",
					zap.Error(err),
					zap.String("addr", ops.addr),
				)
				return
			}
			ops.lg.Info(
				"ListenAndServe exit",
				zap.String("addr", ops.addr),
				zap.String("service", ss.opts.container.Service()),
			)
		}()
	}

	return &ss, nil
}

func (ss *ShardServer) Close() {
	ss.close()

	ss.opts.lg.Info(
		"shardserver: closed",
		zap.String("service", ss.opts.container.Service()),
	)
}

func (ss *ShardServer) close() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.closed {
		return
	}

	// TODO ctx在interface中限定，但是没有用处
	ctx := context.TODO()

	// 保证shard回收的手段，允许调用方启动for不断尝试重新加入存活container中
	// FIXME session会触发drop动作，不允许失败，但也是潜在风险，一般的sdk使用者，不了解close的机制
	dropFn := func(k, v []byte) error {
		shardId := string(k)
		return ss.opts.impl.Drop(shardId)
	}
	if err := ss.keeper.forEach(dropFn); err != nil {
		ss.opts.lg.Error(
			"Drop error",
			zap.String("service", ss.opts.container.Service()),
			zap.Error(err),
		)
	}
	ss.keeper.Close()

	if ss.srv != nil {
		if err := ss.srv.Shutdown(ctx); err != nil {
			ss.opts.lg.Error(
				"Shutdown error",
				zap.Error(err),
				zap.String("service", ss.opts.container.Service()),
			)

		} else {
			ss.opts.lg.Info(
				"Shutdown success",
				zap.String("service", ss.opts.container.Service()),
			)
		}
	}
	if ss.stopper != nil {
		ss.stopper.Close()
	}
	close(ss.donec)

	ss.opts.lg.Info("shard server closed", zap.String("service", ss.opts.container.Service()))
}

func (ss *ShardServer) Done() <-chan struct{} {
	return ss.donec
}

func (ss *ShardServer) Container() *Container {
	return ss.opts.container
}

func (ss *ShardServer) AddShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		ss.opts.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeAdd

	shardNode := EtcdPathAppShardId(ss.opts.container.Service(), req.Id)
	sresp, err := ss.opts.container.Client.GetKV(context.TODO(), shardNode, nil)
	if err != nil {
		ss.opts.lg.Error("GetKV err",
			zap.Error(err),
			zap.String("shardNode", shardNode),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var spec ShardSpec
	if err := json.Unmarshal(sresp.Kvs[0].Value, &spec); err != nil {
		ss.opts.lg.Error("Unmarshal err",
			zap.Error(err),
			zap.ByteString("value", sresp.Kvs[0].Value),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// shard属性校验
	if err := spec.Validate(); err != nil {
		ss.opts.lg.Error("shard spec etcd value err",
			zap.Error(err),
			zap.String("value", string(sresp.Kvs[0].Value)),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// container校验
	if spec.ManualContainerId != "" && spec.ManualContainerId != ss.opts.container.Id() {
		ss.opts.lg.Error("unexpected container for shard",
			zap.String("service", ss.opts.container.Service()),
			zap.String("actual", ss.opts.container.Id()),
			zap.String("expect", spec.ManualContainerId),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "unexpected container"})
		return
	}

	if err := ss.keeper.Add(req.Id, &spec); err != nil {
		ss.opts.lg.Error("Add err",
			zap.Error(err),
			zap.String("id", req.Id),
			zap.Reflect("spec", spec),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ss.opts.lg.Info("add shard request success", zap.Reflect("request", req))

	c.JSON(http.StatusOK, gin.H{})
}

func (ss *ShardServer) DropShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		ss.opts.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeDrop

	if err := ss.keeper.Drop(req.Id); err != nil {
		ss.opts.lg.Error("Drop err",
			zap.Error(err),
			zap.String("id", req.Id),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ss.opts.lg.Info("drop shard success", zap.Reflect("req", req))
	c.JSON(http.StatusOK, gin.H{})
}
