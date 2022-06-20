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
	"sync"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	swaggerfiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	_ apputil.ShardInterface = new(smContainer)
)

// smContainer 竞争leader，管理sm整个集群
type smContainer struct {
	*apputil.Container

	// Client 先初始化，提供给 apputil.Container，一旦 apputil.NewContainer 被调用，
	// shardKeeper就开始干活下发shard，但是 smContainer 的shard实现中包含对Client的使用
	Client etcdutil.EtcdWrapper

	lg *zap.Logger
	// nodeManager 管理 smContainer 内部的etcd节点的pfx
	nodeManager *nodeManager

	// mu 保护closed和shards
	mu sync.Mutex
	// closing 利用 stopper 实现的graceful stop，container进入stopped状态
	closing bool
	// shards 存储分片
	shards map[string]Shard

	// stopper 管理campaign
	stopper *apputil.GoroutineStopper

	// leaderShard 保证sm运行健康的goroutine，通过task节点下发任务给op
	leaderShard *smShard

	// shardWrapper 4 unit test，隔离shard和container
	shardWrapper ShardWrapper
}

func newSMContainer(opts *serverOptions) (*smContainer, error) {
	sCtr := smContainer{
		lg:           opts.lg,
		stopper:      &apputil.GoroutineStopper{},
		shards:       make(map[string]Shard),
		shardWrapper: &smShardWrapper{},
		nodeManager:  &nodeManager{smService: opts.service},
	}

	etcdClient, err := etcdutil.NewEtcdClient(opts.endpoints, opts.lg)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	sCtr.Client = etcdClient

	// 判断sm的spec是否存在,如果不存在，那么进行创建,可以通过接口进行参数更改
	spec := smAppSpec{
		Service:    opts.service,
		CreateTime: time.Now().Unix(),
	}
	lease := apputil.Lease{}
	if err := sCtr.Client.CreateAndGet(
		context.TODO(),
		[]string{
			sCtr.nodeManager.nodeServiceSpec(opts.service),
			sCtr.nodeManager.nodeServiceGuard(opts.service),
			sCtr.nodeManager.nodeServiceContainerHb(opts.service),
		},
		[]string{
			spec.String(),
			lease.String(),
			"",
		},
		clientv3.NoLease); err != nil && err != etcdutil.ErrEtcdNodeExist {
		return nil, err
	}

	container, err := apputil.NewContainer(
		apputil.WithLogger(opts.lg),
		apputil.WithService(opts.service),

		// etcd
		apputil.WithEtcdPrefix(opts.etcdPrefix),
		apputil.WithId(opts.id),
		apputil.WithEtcdClient(etcdClient.Client),

		// http server
		apputil.WithAddr(opts.addr),
		// 使用Container内置的http server
		apputil.WithApiHandler(sCtr.getHttpHandlers()),
		// api背后的具体实现
		apputil.WithShardImplementation(&sCtr),
	)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	sCtr.Container = container

	// 竞争leader
	sCtr.stopper.Wrap(
		func(ctx context.Context) {
			sCtr.campaign(ctx)
		},
	)

	return &sCtr, nil
}

func (c *smContainer) getHttpHandlers() map[string]func(c *gin.Context) {
	apiSrv := newSMShardApi(c)
	handlers := make(map[string]func(c *gin.Context))
	handlers["/sm/server/add-spec"] = apiSrv.GinAddSpec
	handlers["/sm/server/del-spec"] = apiSrv.GinDelSpec
	handlers["/sm/server/get-spec"] = apiSrv.GinGetSpec
	handlers["/sm/server/add-shard"] = apiSrv.GinAddShard
	handlers["/sm/server/del-shard"] = apiSrv.GinDelShard
	handlers["/sm/server/get-shard"] = apiSrv.GinGetShard
	handlers["/sm/server/add-worker"] = apiSrv.GinAddWorker
	handlers["/sm/server/del-worker"] = apiSrv.GinDelWorker
	handlers["/sm/server/get-worker"] = apiSrv.GinGetWorker
	handlers["/sm/server/health"] = apiSrv.GinHealth
	handlers["/swagger/*any"] = ginSwagger.WrapHandler(swaggerfiles.Handler)
	return handlers
}

func (c *smContainer) GetShard(service string) (Shard, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ss, ok := c.shards[service]
	if !ok {
		return nil, errors.New("not exist")
	}
	return ss, nil
}

func (c *smContainer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 保证只被停止一次
	if c.closing {
		return apputil.ErrClosing
	}
	c.closing = true

	// 回收sm当前container负责的分片，后面关闭可能的leader身份，
	// 既然处于关闭状态，也不能再接收shard的移动请求，但是此时http api可能还在工作，
	// 其他选举出来的leader可能会下发失败的请求，最大限度避免掉。
	for _, s := range c.shards {
		if s != nil {
			s.Close()
		}
	}

	// 需要判断是否为nil，worker是在竞选leader时初始化的
	if c.leaderShard != nil {
		// stopper的Close会导致leader的重新选举，新的leader开启rebalance，
		// 尽量防止两个leader的工作在运行，所以worker的停止要在stopper之后
		c.leaderShard.Close()
	}

	// 放弃leader竞选的工作，在资源回收之前，保证自己还是leader
	if c.stopper != nil {
		c.stopper.Close()
	}

	c.lg.Info(
		"smContainer closing",
		zap.String("id", c.Id()),
		zap.String("service", c.Service()),
	)
	return nil
}

func (c *smContainer) Add(id string, spec *apputil.ShardSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		c.lg.Info("container closing, give up add",
			zap.String("id", id),
			zap.Reflect("spec", spec),
		)
		// 4 unit test 提升代码分支可测试性
		// 异常情况不吞掉，反馈给server端，server端也不会一直重试，而是等待下次rebalance
		return apputil.ErrClosing
	}

	sd, ok := c.shards[id]
	if ok {
		if sd.Spec().Task == spec.Task {
			c.lg.Info("shard existed and task not changed",
				zap.String("id", id),
				zap.Reflect("spec", spec),
			)
			// 4 unit test 提升代码分支可测试性
			return apputil.ErrExist
		}

		// 判断是否需要更新shard的工作内容，task有变更停掉当前shard，重新启动
		if sd.Spec().Task != spec.Task {
			sd.Close()
			c.lg.Info("shard task changed, current shard closed",
				zap.String("id", id),
				zap.String("cur", sd.Spec().Task),
				zap.String("new", spec.Task),
			)
		}
	}

	spec.Id = id
	shard, err := c.shardWrapper.NewShard(c, spec)
	if err != nil {
		return errors.Wrap(err, "")
	}
	c.shards[id] = shard
	c.lg.Info("shard added",
		zap.String("id", id),
		zap.Reflect("spec", *spec),
	)
	return nil
}

func (c *smContainer) Drop(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		c.lg.Info("container closing, give up drop",
			zap.String("id", id),
			zap.String("service", c.Service()),
		)
		return apputil.ErrClosing
	}

	sd, ok := c.shards[id]
	if !ok {
		c.lg.Info(
			"shard not existed",
			zap.String("id", id),
			zap.String("service", c.Service()),
		)
		return apputil.ErrNotExist
	}
	sd.Close()
	delete(c.shards, id)
	c.lg.Info(
		"shard dropped",
		zap.String("id", id),
		zap.String("service", c.Service()),
	)
	return nil
}

type leaderEtcdValue struct {
	ContainerId string `json:"containerId"`
	CreateTime  int64  `json:"createTime"`
}

func (v *leaderEtcdValue) String() string {
	b, _ := json.Marshal(v)
	return string(b)
}

func (c *smContainer) campaign(ctx context.Context) {
	for {
	loop:
		select {
		case <-ctx.Done():
			c.lg.Info("leader exit campaign", zap.String("service", c.Service()))
			return
		default:
		}

		leaderNodePrefix := c.nodeManager.nodeSMLeader()
		lvalue := leaderEtcdValue{ContainerId: c.Id(), CreateTime: time.Now().Unix()}
		election := concurrency.NewElection(c.Session, leaderNodePrefix)
		if err := election.Campaign(ctx, lvalue.String()); err != nil {
			c.lg.Error(
				"Campaign error",
				zap.String("service", c.Service()),
				zap.Error(err),
			)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}
		c.lg.Info("campaign leader success",
			zap.String("pfx", leaderNodePrefix),
			zap.Int64("lease", int64(c.Session.Lease())),
		)

		// leader有几种情况会重新选举：
		// 1 重启
		// 2 和etcd之间网络问题
		//
		// 新的leader诞生后，面临的整体container的状态：
		// container也在发布中，存活的数量不确定，发布的并行度（推荐是1，虽然在worker给container提供重启时间，但会引起事件排队增加worker负载，这里没做过压测）
		//
		// leader更换，需要重新构建mapper(存活container)，最差情况是一个container不存活，触发rebalance，
		// 旧的container加回来，发现不能lock shard，剔除掉shard即可，所以这块不用等待

		// 检查所有shard应该都被分配container，当前app的配置信息是预先录入etcd的。此时提取该信息，得到所有shard的id，
		// https://github.com/entertainment-venue/sm/wiki/leader%E8%AE%BE%E8%AE%A1%E6%80%9D%E8%B7%AF
		st := shardTask{GovernedService: c.Service()}
		spec := apputil.ShardSpec{Service: c.Service(), Task: st.String()}
		var err error
		c.leaderShard, err = newSMShard(c, &spec)
		if err != nil {
			c.lg.Error(
				"newSMShard error",
				zap.String("service", c.Service()),
				zap.Error(err),
			)
			goto loop
		}

		// block until出现需要放弃leader职权的事件
		c.lg.Info("leader completed op", zap.String("service", c.Service()))
		select {
		case <-ctx.Done():
			c.lg.Info("leader exit", zap.String("service", c.Service()))
			c.leaderShard = nil
			return
		}
	}
}
