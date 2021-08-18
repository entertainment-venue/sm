package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

type shard struct {
	id      string
	service string

	ew *etcdWrapper

	eq *eventQueue
}

type load struct {
	VirtualMemoryStat  *mem.VirtualMemoryStat `json:"virtualMemoryStat"`
	CPUUsedPercent     float64                `json:"cpuUsedPercent"`
	DiskIOCountersStat []*disk.IOCountersStat `json:"diskIOCountersStat"`
	NetIOCountersStat  *net.IOCountersStat    `json:"netIOCountersStat"`
}

func (l *load) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

func (s *shard) heartbeat(ctx context.Context) error {
	// load上报的间隔
	ticker := time.Tick(3 * time.Second)

	for {
	hbLoop:
		select {
		case <-ticker:
		case <-ctx.Done():
			Logger.Printf("heartbeat exit")
			return nil
		}

		// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持

		session, err := concurrency.NewSession(s.ew.etcdClientV3, concurrency.WithTTL(defaultSessionTimeout))
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}

		ld := load{}

		// 内存使用比率
		vm, err := mem.VirtualMemory()
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}
		ld.VirtualMemoryStat = vm

		// cpu使用比率
		cp, err := cpu.Percent(0, false)
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}
		ld.CPUUsedPercent = cp[0]

		// 磁盘io使用比率
		diskIOCounters, err := disk.IOCounters()
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}
		for _, v := range diskIOCounters {
			ld.DiskIOCountersStat = append(ld.DiskIOCountersStat, &v)
		}

		// 网路io使用比率
		netIOCounters, err := net.IOCounters(false)
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}
		ld.NetIOCountersStat = &netIOCounters[0]

		k := s.ew.heartbeatShardIdNode(s.id, false)
		if _, err := s.ew.etcdClientV3.Put(ctx, k, ld.String(), clientv3.WithLease(session.Lease())); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}
	}
}

func (s *shard) shardOwnerLoop(ctx context.Context) {
	waitTickerLoop(
		ctx,
		defaultShardLoopInterval,
		"shardOwnerLoop exit",
		func(ctx context.Context) error {
			return checkShardOwner(
				ctx,
				s.ew,
				s.ew.heartbeatContainerNode(false),
				s.ew.heartbeatShardNode(false))
		},
	)
}

func (s *shard) shardLoadLoop(ctx context.Context) {
	waitWatchLoop(
		ctx,
		s.ew,
		s.ew.heartbeatShardNode(false),
		"shardLoadLoop exit",
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
				qev.typ = evTypeShardUpdate
			} else {
				qev.typ = evTypeShardDel
				// 3s是给服务器container重启的事件
				qev.expect = start.Add(3 * time.Second).Unix()
			}
			s.eq.push(&qev)
			return nil
		},
	)
}

func (s *shard) containerLoadLoop(ctx context.Context) {
	waitWatchLoop(
		ctx,
		s.ew,
		s.ew.heartbeatContainerNode(false),
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
			return nil
		},
	)
}
