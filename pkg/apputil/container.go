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

	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	ErrClosing  = errors.New("closing")
	ErrExist    = errors.New("exist")
	ErrNotExist = errors.New("not exist")
)

type ShardSpec struct {
	// Id 方法传递的时候可以内容可以自识别，否则，添加分片相关的方法的生命一般是下面的样子：
	// newShard(id string, spec *apputil.ShardSpec)
	Id string `json:"id"`

	// Service 标记自己所在服务，不需要去etcd路径中解析，增加spec的描述性质
	Service string `json:"service"`

	// Task service管理的分片任务内容
	Task string `json:"task"`

	UpdateTime int64 `json:"updateTime"`

	// 通过api可以给shard主动分配到某个container
	ManualContainerId string `json:"manualContainerId"`

	// Group 同一个service需要区分不同种类的shard，
	// 这些shard之间不相关的balance到现有container上
	Group string `json:"group"`

	// LeaseID guard lease
	LeaseID clientv3.LeaseID
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

type ShardInterface interface {
	Add(id string, spec *ShardSpec) error
	Drop(id string) error
}

// ShardHttpApi for gin
type ShardHttpApi interface {
	AddShard(c *gin.Context)
	DropShard(c *gin.Context)
}

// Container 1 上报container的load信息，保证container的liveness，才能够参与shard的分配
// 2 与sm交互，下发add和drop给到Shard
type Container struct {
	Client  etcdutil.EtcdWrapper
	Session *concurrency.Session

	// stopper 管理heartbeat
	stopper *GoroutineStopper

	// donec 可以通知调用方
	donec chan struct{}

	mu sync.Mutex
	// closed 导致 Container 被关闭的事件是异步的，需要做保护
	closed bool

	// keeper 代理shard的操作，封装bolt操作进去
	keeper *shardKeeper

	// opts 存储初始化传入的数据
	opts *containerOptions

	// srv 可选http server
	srv *http.Server
}

type containerOptions struct {
	endpoints []string

	// 数据传递
	id      string
	service string
	lg      *zap.Logger

	// 传入 router 允许shard被集成，降低shard接入对app造成的影响。
	// 例如：现有的web项目使用gin，sm把server启动拿过来也不合适。
	router          *gin.Engine
	routeAndHandler map[string]func(c *gin.Context)
	shardHttpApi    ShardHttpApi
	addr            string
	impl            ShardInterface

	// etcdPrefix 作为sharded application的数据存储prefix，能通过acl做限制
	// TODO 配合 etcdPrefix 需要有用户名和密码的字段
	etcdPrefix string

	// client 允许外部传入
	client *clientv3.Client
}

type ContainerOption func(options *containerOptions)

func WithId(v string) ContainerOption {
	return func(co *containerOptions) {
		co.id = v
	}
}

func WithService(v string) ContainerOption {
	return func(co *containerOptions) {
		co.service = v
	}
}

func WithEndpoints(v []string) ContainerOption {
	return func(co *containerOptions) {
		co.endpoints = v
	}
}

func WithLogger(lg *zap.Logger) ContainerOption {
	return func(co *containerOptions) {
		co.lg = lg
	}
}

func WithShardImplementation(v ShardInterface) ContainerOption {
	return func(co *containerOptions) {
		co.impl = v
	}
}

func WithRouter(v *gin.Engine) ContainerOption {
	return func(co *containerOptions) {
		co.router = v
	}
}

func WithApiHandler(v map[string]func(c *gin.Context)) ContainerOption {
	return func(co *containerOptions) {
		co.routeAndHandler = v
	}
}

func WithShardOpReceiver(v ShardHttpApi) ContainerOption {
	return func(co *containerOptions) {
		co.shardHttpApi = v
	}
}

func WithAddr(v string) ContainerOption {
	return func(co *containerOptions) {
		co.addr = v
	}
}

func WithEtcdPrefix(v string) ContainerOption {
	return func(co *containerOptions) {
		co.etcdPrefix = v
	}
}

func WithEtcdClient(v *clientv3.Client) ContainerOption {
	return func(co *containerOptions) {
		co.client = v
	}
}

func NewContainer(opts ...ContainerOption) (*Container, error) {
	ops := &containerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	if ops.id == "" {
		return nil, errors.New("id err")
	}
	if ops.service == "" {
		return nil, errors.New("service err")
	}
	if len(ops.endpoints) == 0 && ops.client == nil {
		return nil, errors.New("endpoints or client must be init")
	}
	if ops.lg == nil {
		return nil, errors.New("lg err")
	}
	// addr 和 router 二选一，否则啥也不用干了
	if ops.addr == "" && ops.router == nil {
		return nil, errors.New("addr err")
	}
	if ops.impl == nil {
		return nil, errors.New("impl err")
	}

	// FIXME 直接刚常量有点粗糙，暂时没有更好的方案
	InitEtcdPrefix(ops.etcdPrefix)

	// 允许传入etcd的client
	var client *etcdutil.EtcdClient
	if ops.client == nil {
		var err error
		client, err = etcdutil.NewEtcdClient(ops.endpoints, ops.lg)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}
	} else {
		client = etcdutil.NewEtcdClientWithClient(ops.client, ops.lg)
	}
	session, err := concurrency.NewSession(client.Client, concurrency.WithTTL(5))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	ops.lg.Info("session opened",
		zap.String("id", ops.id),
		zap.String("service", ops.service),
	)

	c := Container{
		Client:  client,
		Session: session,
		opts:    ops,

		stopper: &GoroutineStopper{},
		donec:   make(chan struct{}),
	}

	// keeper: 向调用方下发shard move指令，提供本地持久存储能力
	keeper, err := newShardKeeper(ops.lg, &c)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	c.keeper = keeper

	// 上报container初始shard状态，初始化同步做一次，
	// shard带有lease属性，lease的状态分几种：
	// 1 lease和server一致，server不会因为本container触发rb
	// 2 lease和server不一致，server会因为本container触发rb
	// 在container的shard的状态上报ok的情况，shardkeeper的逻辑更容易推算
	if err := c.heartbeat(context.TODO()); err != nil {
		// 报错，但不停止
		c.opts.lg.Error(
			"heartbeat error",
			zap.String("service", c.opts.service),
			zap.Error(err),
		)
		return nil, errors.Wrap(err, "")
	}

	// 在server知晓本地shard属性的前提下，开启处理本地shard的goroutine
	c.keeper.watchLease()

	// 通过heartbeat上报数据
	c.stopper.Wrap(
		func(ctx context.Context) {
			TickerLoop(ctx, ops.lg, 3*time.Second, "container stop upload load", c.heartbeat)
		},
	)

	// 1 监控session，关注etcd导致的异常关闭
	// 2 使用donec，关注外部调用Close导致的关闭
	go func() {
		select {
		case <-c.donec:
			// 被动关闭
			c.opts.lg.Info("container: stopper closed",
				zap.String("id", c.Id()),
				zap.String("service", c.Service()),
			)
		case <-c.Session.Done():
			// 主动关闭
			c.close()

			c.opts.lg.Info("container: session closed",
				zap.String("id", c.Id()),
				zap.String("service", c.Service()),
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

	var receiver ShardHttpApi
	if ops.shardHttpApi != nil {
		receiver = ops.shardHttpApi
	} else {
		receiver = &c
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
		c.srv = srv

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
				zap.String("service", c.Service()),
			)
		}()
	}

	return &c, nil
}

func (ctr *Container) Close() {
	ctr.close()

	ctr.opts.lg.Info("container: closed",
		zap.String("id", ctr.Id()),
		zap.String("service", ctr.Service()),
	)
}

func (ctr *Container) close() {
	ctr.mu.Lock()
	defer ctr.mu.Unlock()
	if ctr.closed {
		return
	}

	// 先干掉srv，停止接受协议请求
	if ctr.srv != nil {
		if err := ctr.srv.Shutdown(context.TODO()); err != nil {
			ctr.opts.lg.Error(
				"Shutdown error",
				zap.Error(err),
				zap.String("service", ctr.Service()),
			)
		} else {
			ctr.opts.lg.Info(
				"Shutdown success",
				zap.String("service", ctr.Service()),
			)
		}
	}

	// 保证shard回收的手段，允许调用方启动for不断尝试重新加入存活container中
	// FIXME session会触发drop动作，不允许失败，但也是潜在风险，一般的sdk使用者，不了解close的机制
	dropFn := func(k, v []byte) error {
		shardId := string(k)
		err := ctr.opts.impl.Drop(shardId)
		if err == ErrNotExist {
			return nil
		}
		return err
	}
	if err := ctr.keeper.forEachRead(dropFn); err != nil {
		ctr.opts.lg.Error(
			"Drop error",
			zap.String("service", ctr.Service()),
			zap.Error(err),
		)
	}
	ctr.keeper.Close()

	if ctr.stopper != nil {
		ctr.stopper.Close()
	}
	close(ctr.donec)
}

func (ctr *Container) Done() <-chan struct{} {
	return ctr.donec
}

func (ctr *Container) Id() string {
	return ctr.opts.id
}

func (ctr *Container) Service() string {
	return ctr.opts.service
}

// SetService 4 unit test
func (ctr *Container) SetService(s string) {
	if ctr.opts == nil {
		ctr.opts = &containerOptions{}
	}
	ctr.opts.service = s
}

type Heartbeat struct {
	// Timestamp sm中用于计算container删除事件的等待时间
	Timestamp int64 `json:"timestamp"`
}

func (s *Heartbeat) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type ContainerHeartbeat struct {
	Heartbeat

	// load
	VirtualMemoryStat  *mem.VirtualMemoryStat `json:"virtualMemoryStat"`
	CPUUsedPercent     float64                `json:"cpuUsedPercent"`
	DiskIOCountersStat []*disk.IOCountersStat `json:"diskIOCountersStat"`
	NetIOCountersStat  *net.IOCountersStat    `json:"netIOCountersStat"`

	// Shards 直接带上id和lease，smserver可以基于lease做有效shard的过滤
	// TODO 支持key-range，前提是server端改造rb算法
	Shards []*ShardKeeperDbValue `json:"shards"`
}

func (l *ContainerHeartbeat) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

func (ctr *Container) heartbeat(ctx context.Context) error {
	ld := ContainerHeartbeat{}
	ld.Timestamp = time.Now().Unix()

	// 内存使用比率
	vm, err := mem.VirtualMemory()
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.VirtualMemoryStat = vm

	// cpu使用比率
	cp, err := cpu.Percent(0, false)
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.CPUUsedPercent = cp[0]

	// 磁盘io使用比率
	diskIOCounters, err := disk.IOCounters()
	if err != nil {
		return errors.Wrap(err, "")
	}
	for _, v := range diskIOCounters {
		ld.DiskIOCountersStat = append(ld.DiskIOCountersStat, &v)
	}

	// 网路io使用比率
	netIOCounters, err := net.IOCounters(false)
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.NetIOCountersStat = &netIOCounters[0]

	// 本地分片信息带到hb中
	var shards []*ShardKeeperDbValue
	if err := ctr.keeper.forEachRead(
		func(k, v []byte) error {
			var dv ShardKeeperDbValue
			if err := json.Unmarshal(v, &dv); err != nil {
				return errors.Wrap(err, string(v))
			}
			if dv.Lease == clientv3.NoLease {
				return nil
			}

			// hb时，以boltdb中存储的shard为准，不关注是否已经同步到app，会有sync保证这块的一致性
			// 1 已下发，app和boltdb一致，hb没问题
			// 2 未下发
			// 		要删除，app未停止，hb要同步
			//		要添加，app未开始，将要开始，hb要同步
			shards = append(shards, &dv)
			return nil
		},
	); err != nil {
		return errors.Wrap(err, "")
	}
	ld.Shards = shards

	// https://tangxusc.github.io/blog/2019/05/etcd-lock%E8%AF%A6%E8%A7%A3/
	// 利用etcd内置lock，防止container冲突，这个问题在container应该比较少见，做到heartbeat即可，smserver就可以做
	lockPfx := EtcdPathAppContainerIdHb(ctr.Service(), ctr.Id())
	mutex := concurrency.NewMutex(ctr.Session, lockPfx)
	if err := mutex.Lock(ctr.Client.Ctx()); err != nil {
		return errors.Wrap(err, "")
	}

	// 上传负载和基础信息
	dataPfx := fmt.Sprintf("%s/%x", lockPfx, ctr.Session.Lease())
	if _, err := ctr.Client.Put(ctx, dataPfx, ld.String(), clientv3.WithLease(ctr.Session.Lease())); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

// ShardMessage sm服务下发的分片
type ShardMessage struct {
	Id   string     `json:"id"`
	Spec *ShardSpec `json:"spec"`
}

func (ctr *Container) AddShard(c *gin.Context) {
	var req ShardMessage
	if err := c.ShouldBind(&req); err != nil {
		ctr.opts.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// shard属性校验
	if err := req.Spec.Validate(); err != nil {
		ctr.opts.lg.Error(
			"Validate err",
			zap.Reflect("req", req),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// container校验
	if req.Spec.ManualContainerId != "" && req.Spec.ManualContainerId != ctr.Id() {
		ctr.opts.lg.Error(
			"unexpected container for shard",
			zap.Reflect("req", req),
			zap.String("service", ctr.Service()),
			zap.String("actual", ctr.Id()),
			zap.String("expect", req.Spec.ManualContainerId),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "unexpected container"})
		return
	}

	req.Spec.Id = req.Id
	if err := ctr.keeper.Add(req.Id, req.Spec); err != nil {
		ctr.opts.lg.Error(
			"Add err",
			zap.Reflect("req", req),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctr.opts.lg.Info(
		"add shard success",
		zap.Reflect("req", req),
	)

	c.JSON(http.StatusOK, gin.H{})
}

func (ctr *Container) DropShard(c *gin.Context) {
	var req ShardMessage
	if err := c.ShouldBind(&req); err != nil {
		ctr.opts.lg.Error(
			"ShouldBind err",
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := ctr.keeper.Drop(req.Id); err != nil && err != ErrNotExist {
		ctr.opts.lg.Error(
			"Drop err",
			zap.Error(err),
			zap.String("id", req.Id),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctr.opts.lg.Info(
		"drop shard success",
		zap.Reflect("req", req),
	)
	c.JSON(http.StatusOK, gin.H{})
}
