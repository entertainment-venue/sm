package main

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

type shardLoad struct {
}

func (l *shardLoad) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

// shard需要实现该接口，帮助理解程序设计，不会有app实现多种doer
type Slicer interface {
	HeartbeatKeeper

	// graceful，保证ShardMover能力被执行的关键点
	Stop()

	// 上报sm各shard的load信息，提供给leader用于做计算
	Heartbeat()
}

type shard struct {
	admin

	cr *container

	eq *eventQueue

	// borderland特有的leader节点，负责管理shard manager内部的shard分配和load监控
	leader bool

	stat *shardStat
}

type shardStat struct {
	RPS     int `json:"rps"`
	AvgTime int `json:"avgTime"`
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

	fn := func(ctx context.Context) error {
		// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持

		session, err := concurrency.NewSession(s.cr.ew.client, concurrency.WithTTL(defaultSessionTimeout))
		if err != nil {
			return errors.Wrap(err, "")
		}

		sd := shardLoad{}

		k := s.cr.ew.hbShardIdNode(s.id, false)
		if _, err := s.cr.ew.client.Put(s.ctx, k, sd.String(), clientv3.WithLease(session.Lease())); err != nil {
			return errors.Wrap(err, "")
		}
		return nil
	}

	tickerLoop(s.ctx, defaultShardLoopInterval, "heartbeat exit", fn, &s.wg)
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
