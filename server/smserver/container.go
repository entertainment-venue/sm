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

type smContainer struct {
	*apputil.Container

	// 容器的唯一标记，是container在sm的唯一标记，每次启动得保持不变，防止在分配shard时有问题
	id string

	// 所属业务标记，在etcd中作为数据隔离的path
	service string

	// 管理自己的goroutine
	stopper *apputil.GoroutineStopper

	lg *zap.Logger

	// 保证sm运行健康的goroutine，通过task节点下发任务给op
	lw *Worker

	mu         sync.Mutex
	idAndShard map[string]*smShard

	// 利用 stopper 实现的graceful stop，container进入stopped状态
	stopped bool

	// nodeManager 管理 smContainer 内部的etcd节点的pfx
	nodeManager *nodeManager
}

func newSMContainer(lg *zap.Logger, id, service string, c *apputil.Container) (*smContainer, error) {
	sc := smContainer{
		lg:        lg,
		id:        id,
		service:   service,
		Container: c,

		stopper:     &apputil.GoroutineStopper{},
		idAndShard:  make(map[string]*smShard),
		nodeManager: &nodeManager{smService: service},
	}
	// 判断sm的spec是否存在,如果不存在，那么进行创建,可以通过接口进行参数更改
	if err := c.Client.CreateAndGet(
		context.Background(),
		[]string{sc.nodeManager.nodeServiceSpec(sc.Service())},
		[]string{(&smAppSpec{Service: service, CreateTime: time.Now().Unix()}).String()},
		clientv3.NoLease); err != nil && err != etcdutil.ErrEtcdNodeExist {
		return nil, errors.Wrap(err, "")
	}

	sc.stopper.Wrap(
		func(ctx context.Context) {
			sc.campaign(ctx)
		},
	)

	return &sc, nil
}

func (c *smContainer) Service() string {
	return c.service
}

func (c *smContainer) GetShard(service string) (*smShard, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ss, ok := c.idAndShard[service]
	if !ok {
		return nil, errors.New("not exist")
	}
	return ss, nil
}

func (c *smContainer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stopped = true

	if c.lw != nil {
		c.lw.Close()
	}

	// stop smShard
	for _, s := range c.idAndShard {
		s.Close()
	}

	c.lg.Info("container closed",
		zap.String("id", c.id),
		zap.String("service", c.service),
	)
}

func (c *smContainer) Add(id string, spec *apputil.ShardSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		c.lg.Info("container closed, give up add",
			zap.String("id", id),
			zap.String("service", c.service),
		)
		return nil
	}

	ss, ok := c.idAndShard[id]
	if ok {
		if ss.Spec().Task == spec.Task {
			c.lg.Info("shard already existed and task not changed",
				zap.String("id", id),
				zap.String("service", c.service),
				zap.String("task", spec.Task),
			)
			return nil
		}

		// 判断是否需要更新shard的工作内容，task有变更停掉当前shard，重新启动
		if ss.Spec().Task != spec.Task {
			ss.Close()
			c.lg.Info("shard task changed, current shard closed",
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
	c.idAndShard[id] = shard
	c.lg.Info("shard added",
		zap.String("id", id),
		zap.Reflect("spec", *spec),
	)
	return nil
}

func (c *smContainer) Drop(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		c.lg.Info("container closed, give up drop",
			zap.String("id", id),
			zap.String("service", c.service),
		)
		return nil
	}

	sd, ok := c.idAndShard[id]
	if !ok {
		c.lg.Info("shard not existed", zap.String("id", id))
		return errors.Wrap(errNotExist, "")
	}
	sd.Close()
	delete(c.idAndShard, id)
	c.lg.Info("shard dropped", zap.String("id", id))
	return nil
}

func (c *smContainer) Load(id string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		c.lg.Info("container closed, give up load",
			zap.String("id", id),
			zap.String("service", c.service),
		)
		return "", nil
	}

	sd, ok := c.idAndShard[id]
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
			c.lg.Info("leader exit campaign", zap.String("service", c.service))
			return
		default:
		}

		leaderNodePrefix := c.nodeManager.nodeSMLeader()
		lvalue := leaderEtcdValue{ContainerId: c.id, CreateTime: time.Now().Unix()}
		election := concurrency.NewElection(c.Session, leaderNodePrefix)
		if err := election.Campaign(ctx, lvalue.String()); err != nil {
			c.lg.Error("failed to campaign",
				zap.String("service", c.service),
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
		c.lw, err = newWorker(c.lg, c, c.service)
		if err != nil {
			c.lg.Error(
				"newWorker error",
				zap.String("service", c.service),
				zap.Error(err),
			)
			goto loop
		}

		// block until出现需要放弃leader职权的事件
		c.lg.Info("leader completed op", zap.String("service", c.service))
		select {
		case <-ctx.Done():
			c.lg.Info("leader exit", zap.String("service", c.service))
			c.lw = nil
			return
		}
	}
}
