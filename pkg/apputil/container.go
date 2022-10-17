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
	"sync"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil/core"
	"github.com/entertainment-venue/sm/pkg/apputil/receiver"
	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"github.com/entertainment-venue/sm/pkg/commonutil"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

// Container 1 上报container的load信息，保证container的liveness，才能够参与shard的分配
// 2 与sm交互，下发add和drop给到Shard
type Container struct {
	Client  etcdutil.EtcdWrapper
	Session *concurrency.Session

	// stopper 管理heartbeat
	stopper *commonutil.GoroutineStopper

	// donec 可以通知调用方
	donec chan struct{}

	mu sync.Mutex
	// closed 导致 Container 被关闭的事件是异步的，需要做保护
	closed bool

	// shardKeeper 代理shard的操作，封装bolt操作进去
	shardKeeper *core.ShardKeeper

	// opts 存储初始化传入的数据
	opts *containerOptions
}

type containerOptions struct {
	addr      string
	endpoints []string

	// 数据传递
	id      string
	service string
	lg      *zap.Logger

	appShardImpl core.ShardPrimitives

	// etcdPrefix 作为sharded application的数据存储prefix，能通过acl做限制
	// TODO 配合 etcdPrefix 需要有用户名和密码的字段
	etcdPrefix string

	// client 允许外部传入
	client *clientv3.Client
	// shardDir shard.db的存储路径，默认是当前目录
	shardDir string

	// dropExpiredShard 默认false，分片应用明确决定对lease敏感，才开启
	dropExpiredShard bool

	// storageType 持久存储的类型，默认是boltdb
	storageType storage.StorageType

	// receiver 允许业务方设置，不传递，可以通过opts选择默认的http receiver
	receiver receiver.Receiver
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

func WithShardPrimitives(v core.ShardPrimitives) ContainerOption {
	return func(co *containerOptions) {
		co.appShardImpl = v
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

func WithShardDir(v string) ContainerOption {
	return func(co *containerOptions) {
		co.shardDir = v
	}
}

func WithDropExpiredShard(v bool) ContainerOption {
	return func(co *containerOptions) {
		co.dropExpiredShard = v
	}
}

func WithStorageType(v storage.StorageType) ContainerOption {
	return func(co *containerOptions) {
		co.storageType = v
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
	if ops.appShardImpl == nil {
		return nil, errors.New("impl err")
	}

	// FIXME 直接刚常量有点粗糙，暂时没有更好的方案
	etcdutil.SetPfx(ops.etcdPrefix)

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

		stopper: &commonutil.GoroutineStopper{},
		donec:   make(chan struct{}),
	}

	// 提供默认的receiver
	if ops.receiver == nil {
		if ops.addr == "" {
			err := errors.Errorf("Empty addr")
			ops.lg.Error(
				"Param err",
				zap.String("id", ops.id),
				zap.String("service", ops.service),
				zap.Error(err),
			)
			return nil, errors.Wrap(err, "")
		} else {
			// TODO opts中的receiver在Close时用到
			c.opts.receiver = receiver.NewHttpServer(ops.lg, ops.addr, ops.id)
		}
	}
	return &c, nil
}

func (ctr *Container) Run() error {
	// keeper: 向调用方下发shard move指令，提供本地持久存储能力
	var (
		st  storage.Storage
		err error
	)
	switch ctr.opts.storageType {
	case storage.Boltdb:
		st, err = storage.NewBoltdb(ctr.opts.shardDir, ctr.opts.service, ctr.opts.lg)
	default:
		st, err = storage.NewEtcddb(ctr.opts.service, ctr.opts.id, ctr.Client, ctr.opts.lg)
	}
	if err != nil {
		return errors.Wrap(err, "")
	}

	skOpts := core.ShardKeeperOptions{
		Service:          ctr.opts.service,
		ContainerId:      ctr.opts.id,
		DropExpiredShard: ctr.opts.dropExpiredShard,
		Client:           ctr.Client,
		ShardDir:         ctr.opts.shardDir,
		AppShardImpl:     ctr.opts.appShardImpl,
	}
	ctr.shardKeeper, err = core.NewShardKeeper(ctr.opts.lg, &skOpts, st)
	if err != nil {
		return errors.Wrap(err, "")
	}

	// 上报container初始shard状态，初始化同步做一次，
	// shard带有lease属性，lease的状态分几种：
	// 1 lease和server一致，server不会因为本container触发rb
	// 2 lease和server不一致，server会因为本container触发rb
	// 在container的shard的状态上报ok的情况，shardkeeper的逻辑更容易推算
	if err := ctr.heartbeat(context.TODO()); err != nil {
		// 报错，但不停止
		ctr.opts.lg.Error(
			"heartbeat error",
			zap.String("service", ctr.opts.service),
			zap.Error(err),
		)
		return errors.Wrap(err, "")
	}

	// 在server知晓本地shard属性的前提下，开启处理本地shard的goroutine
	ctr.shardKeeper.WatchLease()

	// 通过heartbeat上报数据
	ctr.stopper.Wrap(
		func(ctx context.Context) {
			commonutil.TickerLoop(ctx, ctr.opts.lg, 3*time.Second, "container stop upload load", ctr.heartbeat)
		},
	)

	// 1 监控session，关注etcd导致的异常关闭
	// 2 使用donec，关注外部调用Close导致的关闭
	go func() {
		select {
		case <-ctr.donec:
			// 被动关闭
			ctr.opts.lg.Info("container: stopper closed",
				zap.String("id", ctr.Id()),
				zap.String("service", ctr.Service()),
			)
		case <-ctr.Session.Done():
			// 主动关闭
			ctr.close()

			ctr.opts.lg.Info("container: session closed",
				zap.String("id", ctr.Id()),
				zap.String("service", ctr.Service()),
			)
		}
	}()

	ctr.opts.receiver.SetShardPrimitives(ctr.shardKeeper)
	if err := ctr.opts.receiver.Start(); err != nil {
		ctr.opts.lg.Error(
			"Start receiver err",
			zap.String("id", ctr.Id()),
			zap.String("service", ctr.Service()),
			zap.Error(err),
		)
		return errors.Wrap(err, "")
	}
	return nil
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
	if ctr.opts.receiver != nil {
		if err := ctr.opts.receiver.Shutdown(); err != nil {
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
	dropFn := func(shardID string, dv *storage.ShardKeeperDbValue) error {
		err := ctr.opts.appShardImpl.Drop(shardID)
		if err == commonutil.ErrNotExist {
			return nil
		}
		return err
	}
	if err := ctr.shardKeeper.Storage().ForEach(dropFn); err != nil {
		ctr.opts.lg.Error(
			"Drop error",
			zap.String("service", ctr.Service()),
			zap.Error(err),
		)
	}
	ctr.shardKeeper.Close()

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
	Shards []*storage.ShardKeeperDbValue `json:"shards"`
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
	var shards []*storage.ShardKeeperDbValue
	if err := ctr.shardKeeper.Storage().ForEach(
		func(shardID string, dv *storage.ShardKeeperDbValue) error {
			if dv.Spec.Lease.EqualTo(storage.NoLease) {
				return nil
			}

			// hb时，以boltdb中存储的shard为准，不关注是否已经同步到app，会有sync保证这块的一致性
			// 1 已下发，app和boltdb一致，hb没问题
			// 2 未下发
			// 		要删除，app未停止，hb要同步
			//		要添加，app未开始，将要开始，hb要同步
			shards = append(shards, dv)
			return nil
		},
	); err != nil {
		return errors.Wrap(err, "")
	}
	ld.Shards = shards

	// https://tangxusc.github.io/blog/2019/05/etcd-lock%E8%AF%A6%E8%A7%A3/
	// 利用etcd内置lock，防止container冲突，这个问题在container应该比较少见，做到heartbeat即可，smserver就可以做
	lockPfx := etcdutil.ContainerPath(ctr.Service(), ctr.Id())
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

func (ctr *Container) Receiver() receiver.Receiver {
	return ctr.opts.receiver
}
