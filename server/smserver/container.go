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
	"golang.org/x/sync/errgroup"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type smContainer struct {
	*apputil.Container

	// 容器的唯一标记，是container在sm的唯一标记，每次启动得保持不变，防止在分配shard时有问题
	id string

	// 所属业务标记，在etcd中作为数据隔离的path
	service string

	// 管理自己的goroutine
	gs *apputil.GoroutineStopper

	lg *zap.Logger

	// 保证sm运行健康的goroutine，通过task节点下发任务给op
	lw *maintenanceWorker

	eq *eventQueue

	mu         sync.Mutex
	idAndShard map[string]*smShard

	// operator 的操作耗时可能较长，为每个service分配一个，防止一个任务stop the world
	shardServiceAndOp map[string]*operator

	// op需要监听特定app的task在etcd中的节点，保证app级别只有一个，sm放在leader中
	op *operator

	// 利用gs实现的graceful stop，container进入stopped状态
	stopped bool
}

func newSMContainer(ctx context.Context, lg *zap.Logger, id, service string, c *apputil.Container) (*smContainer, error) {
	sc := smContainer{
		lg:        lg,
		id:        id,
		service:   service,
		Container: c,

		gs:                &apputil.GoroutineStopper{},
		idAndShard:        make(map[string]*smShard),
		shardServiceAndOp: make(map[string]*operator),
	}

	sc.eq = newEventQueue(ctx, lg, &sc)
	sc.lw = newMaintenanceWorker(ctx, lg, &sc, sc.service)

	sc.gs.Wrap(
		func(ctx context.Context) {
			sc.campaign(ctx)
		})

	return &sc, nil
}

func (c *smContainer) Service() string {
	return c.service
}

func (c *smContainer) EventQueue() *eventQueue {
	return c.eq
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

	// stop operator
	for _, o := range c.shardServiceAndOp {
		o.Close()
	}

	c.lg.Info("container closed",
		zap.String("id", c.id),
		zap.String("service", c.service),
	)
}

func (c *smContainer) Add(ctx context.Context, id string, spec *apputil.ShardSpec) error {
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

	shard, err := newShard(ctx, c.lg, c, id, spec)
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

func (c *smContainer) Drop(_ context.Context, id string) error {
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
	c.lg.Info("shard dropped", zap.String("id", id))
	return nil
}

func (c *smContainer) Load(_ context.Context, id string) (string, error) {
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
	c.lg.Info("get load success",
		zap.String("id", id),
		zap.String("load", load),
	)
	return load, nil
}

func (c *smContainer) Shards(_ context.Context) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ids []string
	for id := range c.idAndShard {
		ids = append(ids, id)
	}
	c.lg.Info("get shards success", zap.String("service", c.service))
	return ids, nil
}

func (c *smContainer) RegisterOperator(shardService string) error {
	// lock不需要，在Add中调用该方法
	if c.stopped {
		c.lg.Info("container closed, give up register op", zap.String("service", shardService))
		return nil
	}

	if _, ok := c.shardServiceAndOp[shardService]; !ok {
		op, err := newOperator(c.lg, c, shardService)
		if err != nil {
			return errors.Wrap(err, "")
		}
		c.shardServiceAndOp[shardService] = op
	}
	c.lg.Info("register operator success", zap.String("shardService", shardService))
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
			c.lg.Info("leader exit campaign", zap.String("service", c.service))
			return
		default:
		}

		leaderNodePrefix := nodeLeader(c.service)
		lvalue := leaderEtcdValue{ContainerId: c.id, CreateTime: time.Now().Unix()}
		election := concurrency.NewElection(c.Session, leaderNodePrefix)
		if err := election.Campaign(ctx, lvalue.String()); err != nil {
			c.lg.Error("failed to campaign",
				zap.Error(err),
				zap.String("service", c.service),
			)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}
		c.lg.Info("campaign leader success",
			zap.String("node", leaderNodePrefix),
			zap.Int64("lease", int64(c.Session.Lease())),
		)

		// leader启动时，等待一个时间段，方便所有container做至少一次heartbeat，然后开始监测是否需要进行container和shard映射关系的变更。
		// etcd sdk中keepalive的请求发送时间时500ms，5s>>500ms，认为这个时间段内，所有container都会发heartbeat，不存在的就认为没有任务。
		time.Sleep(5 * time.Second)

		// leader需要处理shard move的任务
		var err error
		c.op, err = newOperator(c.lg, c, c.service)
		if err != nil {
			c.lg.Error("leader failed to newOperator", zap.Error(err))
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		if err := c.initDistribute(ctx); err != nil {
			c.lg.Error("leader failed to initDistribute", zap.Error(err))
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		// 初始化完毕再开始处理其他的loop检测到的shard move action
		c.op.Start()

		// 检查所有shard应该都被分配container，当前app的配置信息是预先录入etcd的。此时提取该信息，得到所有shard的id，
		// https://github.com/entertainment-venue/sm/wiki/leader%E8%AE%BE%E8%AE%A1%E6%80%9D%E8%B7%AF
		c.lw = newMaintenanceWorker(ctx, c.lg, c, c.service)

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

func (c *smContainer) initDistribute(ctx context.Context) error {
	// 先把当前的分配关系下发下去，和static membership，不过我们场景是由单点完成的，由性能瓶颈，但是不像LRMF场景下serverless难以判断正确性
	// 需要交互的下游是sm的机器节点，需要根据负载来看，预估不会很多
	bdShardNode := nodeAppShard(c.service)
	curShardIdAndValue, err := c.Client.GetKVs(ctx, bdShardNode)
	if err != nil {
		return errors.Wrap(err, "")
	}
	var moveActions moveActionList
	for shardId, value := range curShardIdAndValue {
		var ss apputil.ShardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}

		// 未分配container的shard，不需要move指令下发
		if ss.ContainerId != "" {
			// 下发指令，接受不了的直接干掉当前的分配关系
			ma := moveAction{Service: c.service, ShardId: shardId, AddEndpoint: ss.ContainerId, AllowDrop: true}
			moveActions = append(moveActions, &ma)

			c.lg.Info("leader init shard move action",
				zap.String("service", c.service),
				zap.Reflect("action", ma),
			)
		}
	}
	// 向自己的app任务节点发任务
	if len(moveActions) == 0 {
		c.lg.Info("initDistribute no move action created", zap.String("service", c.service))
		return nil
	}

	// leader下发指令失败，在campaign中不断实验，重新走上面的逻辑，获得当前分片信息，存在人工接入修正分片spec的可能
	g := new(errgroup.Group)
	for _, ma := range moveActions {
		ma := ma
		g.Go(func() error {
			return c.op.dropOrAdd(ctx, ma)
		})
	}
	if err := g.Wait(); err != nil {
		time.Sleep(defaultSleepTimeout)
		return errors.Wrap(err, "")
	}
	return nil
}
