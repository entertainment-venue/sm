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
type Mover interface {
	HeartbeatKeeper

	Add(ctx context.Context, id string) error
	Drop(ctx context.Context, id string) error
}

type container struct {
	admin

	service string

	ew *etcdWrapper

	mu     sync.Mutex
	shards map[string]*shard
}

func (c *container) campaignLeader(ctx context.Context) error {
	ew, err := newEtcdWrapper([]string{})
	if err != nil {
		return errors.Wrap(err, "")
	}

	for {
	campaignLoop:
		select {
		case <-ctx.Done():
			Logger.Printf("campaignLeader exit")
			return nil
		default:
		}

		session, err := concurrency.NewSession(ew.etcdClientV3, concurrency.WithTTL(defaultSessionTimeout))
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto campaignLoop
		}

		election := concurrency.NewElection(session, c.ew.leaderNode())
		if err := election.Campaign(ctx, c.id); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto campaignLoop
		}

		Logger.Printf("Successfully campaign for current container %s", c.id)

		// leader启动时，等待一个时间段，方便所有container做至少一次heartbeat，然后开始监测是否需要进行container和shard映射关系的变更。
		// etcd sdk中keepalive的请求发送时间时500ms，3s>>500ms，认为这个时间段内，所有container都会发heartbeat，不存在的就认为没有任务。
		time.Sleep(3 * time.Second)

		// 检查所有shard应该都被分配container，当前app的配置信息是预先录入etcd的。此时提取该信息，得到所有shard的id，
		// https://github.com/entertainment-venue/borderland/wiki/leader%E8%AE%BE%E8%AE%A1%E6%80%9D%E8%B7%AF
	}
}

func (c *container) Add(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.shards[id]; ok {
		return errAlreadyExist
	}
	c.shards[id] = newShard(id, c)
	return nil
}

func (c *container) Drop(ctx context.Context, id string) error {
	sd, ok := c.shards[id]
	if !ok {
		return errNotExist
	}
	sd.Stop()
	return nil
}

func (c *container) Heartbeat() {
	defer c.wg.Done()

	fn := func(ctx context.Context) error {
		// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持

		session, err := concurrency.NewSession(c.ew.etcdClientV3, concurrency.WithTTL(defaultSessionTimeout))
		if err != nil {
			return errors.Wrap(err, "")
		}

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

		k := c.ew.hbContainerIdNode(c.id, false)
		if _, err := c.ew.etcdClientV3.Put(c.ctx, k, ld.String(), clientv3.WithLease(session.Lease())); err != nil {
			return errors.Wrap(err, "")
		}
		return nil
	}

	tickerLoop(
		c.ctx,
		defaultShardLoopInterval,
		"heartbeat exit",
		fn,
		&c.wg,
	)
}
