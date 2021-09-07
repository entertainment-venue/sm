package server

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

type sysLoad struct {
	VirtualMemoryStat  *mem.VirtualMemoryStat `json:"virtualMemoryStat"`
	CPUUsedPercent     float64                `json:"cpuUsedPercent"`
	DiskIOCountersStat []*disk.IOCountersStat `json:"diskIOCountersStat"`
	NetIOCountersStat  *net.IOCountersStat    `json:"netIOCountersStat"`
}

func (l *sysLoad) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

// container需要实现该接口，作为管理指令的接收点
type Container interface {
	Closer
	LoadUploader

	Add(id string) error
	Drop(id string) error
}

type container struct {
	goroutineStopper

	id string

	// 所属app的service name
	service string

	ew *etcdWrapper

	mu     sync.Mutex
	shards map[string]Sharder

	// shard borderland管理很多业务app，不同业务app有不同的task节点，这块做个map，可能出现单container负责多个app的场景
	srvOps map[string]Operator

	eq *eventQueue

	leader *leader

	// 可以被leader或者follower占用
	session *concurrency.Session
}

func newContainer(id, service string, endpoints []string) (*container, error) {
	var (
		err error
		ctr container
	)

	ctr.service = service
	ctr.id = id
	ctr.ctx, ctr.cancel = context.WithCancel(context.Background())

	ctr.ew, err = newEtcdWrapper(endpoints, &ctr)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持
	ctr.session, err = concurrency.NewSession(ctr.ew.client, concurrency.WithTTL(defaultSessionTimeout))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	go func() {
		defer ctr.Close()
		for range ctr.session.Done() {

		}
		Logger.Printf("container %s session closed", ctr.id)
	}()

	go ctr.UploadLoad()

	ctr.eq = newEventQueue(ctr.ctx)

	ctr.leader = newLeader(&ctr)

	return &ctr, nil
}

func (c *container) Close() {
	for _, s := range c.shards {
		s.Close()
	}

	if c.leader != nil {
		c.leader.close()
	}

	c.cancel()
	c.wg.Wait()

	Logger.Printf("container %s for service %s stopped", c.id, c.service)
}

func (c *container) UploadLoad() {
	defer c.wg.Done()

	fn := func(ctx context.Context) error {
		ld := sysLoad{}

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

		k := c.ew.nodeAppContainerIdHb(c.service, c.id)
		if _, err := c.ew.client.Put(c.ctx, k, ld.String(), clientv3.WithLease(c.session.Lease())); err != nil {
			return errors.Wrap(err, "")
		}

		return nil
	}

	tickerLoop(c.ctx, defaultShardLoopInterval, "Container upload exit", fn, &c.wg)
}

func (c *container) Add(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.shards[id]; ok {
		Logger.Printf("shard %s already added", id)
		// 允许重入，Add操作保证at least once
		return nil
	}
	shard, err := newShard(id, c)
	if err != nil {
		return errors.Wrap(err, "")
	}
	c.shards[id] = shard
	return nil
}

func (c *container) Drop(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sd, ok := c.shards[id]
	if !ok {
		return errNotExist
	}
	sd.Close()
	return nil
}

func (c *container) TryNewOp(service string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.srvOps[service]; !ok {
		op, err := newOperator(c)
		if err != nil {
			return errors.Wrap(err, "")
		}
		c.srvOps[service] = op
	}
	return nil
}
