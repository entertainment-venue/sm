package server

import (
	"context"
	"sync"

	"github.com/entertainment-venue/sm/pkg/apputil"
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
	shards map[string]*serverShard
	// serverShard sm管理很多业务app，不同业务app有不同的task节点，这块做个map，可能出现单container负责多个app的场景
	srvOps  map[string]*operator
	stopped bool // container进入stopped状态

	eq *eventQueue

	leader *leader
}

func newServerContainer(ctx context.Context, id, service string, endpoints []string) (*serverContainer, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Container只关注通用部分，所以service和id还是要保留一份到数据结构
	sc := serverContainer{service: service, id: id, cancel: cancel, ew: newEtcdWrapper(), eq: newEventQueue(ctx)}

	sc.leader = newLeader(ctx, &sc)

	return &sc, nil
}

func (c *serverContainer) Close() {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()

	// stop leader
	if c.leader != nil {
		c.leader.close()
	}

	// stop serverShard
	for _, s := range c.shards {
		s.Close()
	}

	// stop operator
	for _, o := range c.srvOps {
		o.Close()
	}

	Logger.Printf("serverContainer %s for service %s stopped", c.id, c.service)
}

func (c *serverContainer) Add(_ context.Context, id string, spec *apputil.ShardSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		Logger.Printf("[parent] service %s stopped, id %s", c.service, id)
		return nil
	}

	if _, ok := c.shards[id]; ok {
		Logger.Printf("serverShard %s already added", id)
		// 允许重入，Add操作保证at least once
		return nil
	}

	shard, err := startShard(context.TODO(), c, id, spec)
	if err != nil {
		return errors.Wrap(err, "")
	}
	c.shards[id] = shard
	return nil
}

func (c *serverContainer) Drop(_ context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		Logger.Printf("[parent] service %s stopped, id %s", c.service, id)
		return nil
	}

	sd, ok := c.shards[id]
	if !ok {
		return errNotExist
	}
	sd.Close()
	return nil
}

func (c *serverContainer) Load(ctx context.Context, id string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		Logger.Printf("[parent] service %s stopped, id %s", c.service, id)
		return "", nil
	}

	sd, ok := c.shards[id]
	if !ok {
		return "", errNotExist
	}
	return sd.getLoad(), nil
}

func (c *serverContainer) NewOp(service string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		Logger.Printf("[parent] service %s stopped", service)
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
