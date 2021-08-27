package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

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
	admin

	ew *etcdWrapper

	mu     sync.Mutex
	shards map[string]Shard

	op Operator

	session *concurrency.Session

	eq *eventQueue
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

	ctr.op, err = newOperator(&ctr)
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

	go ctr.campaign()

	go ctr.Upload()

	return &ctr, nil
}

func (c *container) campaign() {
	for {
	loop:
		select {
		case <-c.ctx.Done():
			Logger.Printf("campaign exit")
			return
		default:
		}

		election := concurrency.NewElection(c.session, c.ew.leaderNode())
		if err := election.Campaign(c.ctx, c.id); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		Logger.Printf("Successfully campaign for current ctr %s", c.id)

		// leader启动时，等待一个时间段，方便所有container做至少一次heartbeat，然后开始监测是否需要进行container和shard映射关系的变更。
		// etcd sdk中keepalive的请求发送时间时500ms，3s>>500ms，认为这个时间段内，所有container都会发heartbeat，不存在的就认为没有任务。
		time.Sleep(15 * time.Second)

		// 先把当前的分配关系下发下去，和static membership，不过我们场景是由单点完成的，由性能瓶颈，但是不像LRMF场景下serverless难以判断正确性
		// 分配关系下发，解决的是先把现有分配关系搞下去，然后再通过shardAllocateLoop检验是否需要整体进行shard move，相当于init
		// TODO app接入数量一个公司可控，所以方案可行
		shardIdAndValue, err := c.ew.getKvs(c.ctx, c.ew.nodeAppShard(c.service))
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}
		var moveActions moveActionList
		for shardId, value := range shardIdAndValue {
			var ss shardSpec
			if err := json.Unmarshal([]byte(value), &ss); err != nil {
				Logger.Printf("err %+v", err)
				time.Sleep(defaultSleepTimeout)
				goto loop
			}

			// 下发指令，接受不了的直接干掉当前的分配关系
			moveActions = append(moveActions, &moveAction{
				Service:     c.service,
				ShardId:     shardId,
				AddEndpoint: ss.ContainerId,
				AllowDrop:   true,
			})
		}
		// 向自己的app任务节点发任务
		if _, err := c.ew.compareAndSwap(c.ctx, c.ew.nodeAppTask(c.service), "", moveActions.String(), -1); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		// 检查所有shard应该都被分配container，当前app的配置信息是预先录入etcd的。此时提取该信息，得到所有shard的id，
		// https://github.com/entertainment-venue/borderland/wiki/leader%E8%AE%BE%E8%AE%A1%E6%80%9D%E8%B7%AF
		go c.shardAllocateLoop()

		go c.containerLoadLoop()
	}
}

func (c *container) shardAllocateLoop() {
	tickerLoop(
		c.ctx,
		defaultShardLoopInterval,
		"shardAllocateLoop exit",
		func(ctx context.Context) error {
			return shardAllocateChecker(ctx, c.ew, c.service)
		},
		&c.wg,
	)
}

func (c *container) shardLoadLoop() {
	watchLoop(
		c.ctx,
		c.ew,
		c.ew.nodeAppShardHb(c.service),
		"shardLoadLoop exit",
		func(ctx context.Context, ev *clientv3.Event) error {
			return shardLoadChecker(ctx, c.eq, ev)
		},
		&c.wg,
	)
}

func (c *container) containerLoadLoop() {
	watchLoop(
		c.ctx,
		c.ew,
		c.ew.nodeAppContainerHb(c.service),
		"containerLoadLoop exit",
		func(ctx context.Context, ev *clientv3.Event) error {
			if ev.IsCreate() {
				return nil
			}

			start := time.Now()
			qev := event{
				start: start.Unix(),
				load:  string(ev.Kv.Value),
			}

			if ev.IsModify() {
				qev.typ = evTypeContainerUpdate
			} else {
				qev.typ = evTypeContainerDel
				// 3s是给服务器container重启的事件
				qev.expect = start.Add(3 * time.Second).Unix()
			}
			c.eq.push(&qev)
			return nil
		},
		&c.wg,
	)
}

func (c *container) Close() {
	c.op.Close()

	for _, s := range c.shards {
		s.Close()
	}

	c.cancel()
	c.wg.Wait()

	Logger.Printf("container %s for service %s stopped", c.id, c.service)
}

func (c *container) Upload() {
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
