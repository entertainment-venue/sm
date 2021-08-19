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

type shard struct {
	admin

	cr *container

	eq *eventQueue

	// borderland特有的leader节点，负责管理shard manager内部的shard分配和load监控
	leader bool
}

func newShard(id string, cr *container) *shard {
	s := shard{cr: cr}
	s.id = id
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.eq = newEventQueue(s.ctx)

	s.wg.Add(3)
	go s.Heartbeat()
	go s.allocateLoop()
	go s.loadLoop()
	return &s
}

func (s *shard) Stop() {
	s.cancel()
	s.wg.Wait()
	Logger.Printf("shard %s for service %s stopped", s.id, s.cr.service)
}

func (s *shard) Heartbeat() {
	defer s.wg.Done()

	// load上报的间隔
	ticker := time.Tick(3 * time.Second)

	for {
	hbLoop:
		select {
		case <-ticker:
		case <-s.ctx.Done():
			Logger.Printf("heartbeat exit")
			return
		}

		// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持

		session, err := concurrency.NewSession(s.cr.ew.etcdClientV3, concurrency.WithTTL(defaultSessionTimeout))
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

		k := s.cr.ew.hbShardIdNode(s.id, false)
		if _, err := s.cr.ew.etcdClientV3.Put(s.ctx, k, ld.String(), clientv3.WithLease(session.Lease())); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}
	}
}

func (s *shard) allocateLoop() {
	tickerLoop(
		s.ctx,
		defaultShardLoopInterval,
		"allocateLoop exit",
		func(ctx context.Context) error {
			return checkShardOwner(
				ctx,
				s.cr.ew,
				s.cr.ew.hbContainerNode(s.leader),
				s.cr.ew.hbShardNode(s.leader))
		},
		&s.wg,
	)
}

func (s *shard) loadLoop() {
	watchLoop(
		s.ctx,
		s.cr.ew,
		s.cr.ew.hbShardNode(s.leader),
		"loadLoop exit",
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
		&s.wg,
	)
}
