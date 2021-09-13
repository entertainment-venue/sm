package server

import (
	"context"
	"sync"

	"github.com/entertainment-venue/borderland/pkg/apputil"
	"github.com/pkg/errors"
)

type serverContainer struct {
	*apputil.Container

	// 存下来，方便一些管理逻辑
	id, service string
	// 管理孩子goroutine
	cancel context.CancelFunc

	// 管理自己的goroutine
	stopper *apputil.GoroutineStopper

	ew *etcdWrapper

	mu     sync.Mutex
	shards map[string]*shard
	// shard borderland管理很多业务app，不同业务app有不同的task节点，这块做个map，可能出现单container负责多个app的场景
	srvOps  map[string]*operator
	stopped bool // container进入stopped状态

	eq *eventQueue

	leader *leader
}

func newContainer(ctx context.Context, id, service string, endpoints []string) (*serverContainer, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Container只关注通用部分，所以service和id还是要保留一份到数据结构
	sc := serverContainer{service: service, id: id, cancel: cancel, ew: newEtcdWrapper()}

	cc, err := apputil.NewContainer(
		apputil.WithContext(ctx),
		apputil.WithService(service),
		apputil.WithId(id),
		apputil.WithEndpoints(endpoints))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	go func() {
		// graceful stop serverContainer
		defer sc.Close()

		// 监控apputil中的Container，退出后，保证liveness之外的功能也需要退出，stop the world
		for range cc.Done() {

		}

		sc.mu.Lock()
		sc.stopped = true
		sc.mu.Unlock()
	}()

	sc.eq = newEventQueue(ctx)
	sc.leader = newLeader(ctx, &sc)

	return &sc, nil
}

func (c *serverContainer) Close() {
	// stop leader
	if c.leader != nil {
		c.leader.close()
	}

	// stop shard
	for _, s := range c.shards {
		s.Close()
	}

	// stop operator
	for _, o := range c.srvOps {
		o.Close()
	}

	Logger.Printf("serverContainer %s for service %s stopped", c.id, c.service)
}

func (c *serverContainer) Add(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		Logger.Printf("[sc] service %s stopped, id %s", c.service, id)
		return nil
	}

	if _, ok := c.shards[id]; ok {
		Logger.Printf("shard %s already added", id)
		// 允许重入，Add操作保证at least once
		return nil
	}

	shard, err := startShard(context.TODO(), id, c)
	if err != nil {
		return errors.Wrap(err, "")
	}
	c.shards[id] = shard
	return nil
}

func (c *serverContainer) Drop(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		Logger.Printf("[sc] service %s stopped, id %s", c.service, id)
		return nil
	}

	sd, ok := c.shards[id]
	if !ok {
		return errNotExist
	}
	sd.Close()
	return nil
}

func (c *serverContainer) NewOp(service string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		Logger.Printf("[sc] service %s stopped", service)
		return nil
	}

	if _, ok := c.srvOps[service]; !ok {
		op, err := newOperator(c, service)
		if err != nil {
			return errors.Wrap(err, "")
		}
		c.srvOps[service] = op
	}
	return nil
}
