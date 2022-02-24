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
	"github.com/pkg/errors"
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

	lg *zap.Logger
	// nodeManager 管理 smContainer 内部的etcd节点的pfx
	nodeManager *nodeManager

	// mu 保护closed和shards
	mu sync.Mutex
	// closing 利用 stopper 实现的graceful stop，container进入stopped状态
	closing bool
	// shards 存储分片
	shards map[string]*smShard

	// stopper 管理campaign
	stopper *apputil.GoroutineStopper

	// worker 保证sm运行健康的goroutine，通过task节点下发任务给op
	worker *Worker
}

func newSMContainer(lg *zap.Logger, c *apputil.Container) (*smContainer, error) {
	container := smContainer{
		lg:        lg,
		Container: c,

		stopper:     &apputil.GoroutineStopper{},
		shards:      make(map[string]*smShard),
		nodeManager: &nodeManager{smService: c.Service()},
	}
	// 判断sm的spec是否存在,如果不存在，那么进行创建,可以通过接口进行参数更改
	spec := smAppSpec{Service: c.Service(), CreateTime: time.Now().Unix()}
	if err := c.Client.CreateAndGet(
		context.TODO(),
		[]string{container.nodeManager.nodeServiceSpec(container.Service())},
		[]string{spec.String()},
		clientv3.NoLease,
	); err != nil && err != etcdutil.ErrEtcdNodeExist {
		return nil, errors.Wrap(err, "")
	}

	container.stopper.Wrap(
		func(ctx context.Context) {
			container.campaign(ctx)
		},
	)

	return &container, nil
}

func (c *smContainer) GetShard(service string) (*smShard, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ss, ok := c.shards[service]
	if !ok {
		return nil, errors.New("not exist")
	}
	return ss, nil
}

func (c *smContainer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 保证只被停止一次
	if c.closing {
		return
	}
	c.closing = true

	// 回收sm当前container负责的分片，后面关闭可能的leader身份，
	// 既然处于关闭状态，也不能再接收shard的移动请求，但是此时http api可能还在工作，
	// 其他选举出来的leader可能会下发失败的请求，最大限度避免掉。
	for _, s := range c.shards {
		s.Close()
	}

	// 需要判断是否为nil，worker是在竞选leader时初始化的
	if c.worker != nil {
		// stopper的Close会导致leader的重新选举，新的leader开启rebalance，
		// 尽量防止两个leader的工作在运行，所以worker的停止要在stopper之后
		c.worker.Close()
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
}

func (c *smContainer) Add(id string, spec *apputil.ShardSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		c.lg.Info("container closing, give up add",
			zap.String("id", id),
			zap.String("service", c.Service()),
		)
		return nil
	}

	ss, ok := c.shards[id]
	if ok {
		if ss.Spec().Task == spec.Task {
			c.lg.Info("shard already existed and task not changed",
				zap.String("id", id),
				zap.String("service", c.Service()),
				zap.String("task", spec.Task),
			)
			return nil
		}

		// 判断是否需要更新shard的工作内容，task有变更停掉当前shard，重新启动
		if ss.Spec().Task != spec.Task {
			ss.Close()
			c.lg.Info("shard task changed, current shard closing",
				zap.String("id", id),
				zap.String("cur", ss.Spec().Task),
				zap.String("new", spec.Task),
			)
		}
	}

	shard, err := newShard(c.lg, c, id, spec)
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
		return nil
	}

	sd, ok := c.shards[id]
	if !ok {
		c.lg.Info("shard not existed", zap.String("id", id))
		return errors.Wrap(errNotExist, "")
	}
	sd.Close()
	delete(c.shards, id)
	c.lg.Info("shard dropped", zap.String("id", id))
	return nil
}

func (c *smContainer) Load(id string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closing {
		c.lg.Info("container closing, give up load",
			zap.String("id", id),
			zap.String("service", c.Service()),
		)
		return "", nil
	}

	sd, ok := c.shards[id]
	if !ok {
		c.lg.Warn("shard not exist", zap.String("id", id))
		return "", errors.Wrap(errNotExist, "")
	}
	load := sd.GetLoad()
	c.lg.Debug("get load success",
		zap.String("id", id),
		zap.String("load", load),
	)
	return load, nil
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

		// leader启动时，等待一个时间段，方便所有container做至少一次heartbeat，然后开始监测是否需要进行container和shard映射关系的变更。
		// etcd sdk中keepalive的请求发送时间时500ms，5s>>500ms，认为这个时间段内，所有container都会发heartbeat，不存在的就认为没有任务。
		time.Sleep(defaultMaxRecoveryTime)

		// 检查所有shard应该都被分配container，当前app的配置信息是预先录入etcd的。此时提取该信息，得到所有shard的id，
		// https://github.com/entertainment-venue/sm/wiki/leader%E8%AE%BE%E8%AE%A1%E6%80%9D%E8%B7%AF
		var err error
		c.worker, err = newWorker(c.lg, c, c.Service())
		if err != nil {
			c.lg.Error(
				"newWorker error",
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
			c.worker = nil
			return
		}
	}
}
