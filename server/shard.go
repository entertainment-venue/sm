package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

type shardLoad struct {
}

func (l *shardLoad) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

// shard需要实现该接口，帮助理解程序设计，不会有app实现多种doer
type Shard interface {
	Closer
	LoadUploader

	StayHealthy()
}

type shard struct {
	admin

	cr *container

	eq *eventQueue

	stat *shardStat

	session *concurrency.Session
}

type shardStat struct {
	RPS     int `json:"rps"`
	AvgTime int `json:"avgTime"`
}

func newShard(id string, cr *container) (*shard, error) {
	s := shard{cr: cr}
	s.id = id
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.eq = newEventQueue(s.ctx)

	var err error
	s.session, err = concurrency.NewSession(s.cr.ew.client, concurrency.WithTTL(defaultSessionTimeout))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	// 保持shard级别session的重试
	go func() {
		defer s.Close()
		for range s.session.Done() {

		}
		Logger.Printf("shard %s session closed", s.id)
	}()

	// 获取shard的任务信息，在sm场景下，shard中包含所负责的app的service信息
	resp, err := s.cr.ew.get(s.ctx, s.cr.ew.nodeAppShardId(s.cr.service, s.id), nil)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		err = errors.Errorf("Failed to get shard %s content", s.id)
		return nil, errors.Wrap(err, "")
	}
	ss := shardSpec{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &ss); err != nil {
		return nil, errors.Wrap(err, "")
	}
	s.service = ss.Service

	s.StayHealthy()

	return &s, nil
}

func (s *shard) Close() {
	s.cancel()
	s.wg.Wait()
	Logger.Printf("shard %s for service %s stopped", s.id, s.cr.service)
}

func (s *shard) Upload() {
	defer s.wg.Done()

	fn := func(ctx context.Context) error {
		sd := shardLoad{}

		// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持
		k := s.cr.ew.nodeAppShardHbId(s.cr.service, s.id)
		if _, err := s.cr.ew.client.Put(s.ctx, k, sd.String(), clientv3.WithLease(s.session.Lease())); err != nil {
			return errors.Wrap(err, "")
		}
		return nil
	}

	tickerLoop(s.ctx, defaultShardLoopInterval, "heartbeat exit", fn, &s.wg)
}

func (s *shard) StayHealthy() {
	s.wg.Add(3)
	go s.Upload()

	// 检查app的分配和app shard上传的负载是否健康
	go s.appShardAllocateLoop()
	go s.appShardLoadLoop()
}

func (s *shard) appShardAllocateLoop() {
	tickerLoop(
		s.ctx,
		defaultShardLoopInterval,
		"appShardAllocateLoop exit",
		func(ctx context.Context) error {
			return shardAllocateChecker(
				ctx,
				s.cr.ew,
				s.cr.ew.nodeAppHbContainer(s.service),
				s.cr.ew.nodeAppShard(s.service),
				s.cr.ew.nodeAppTask(s.service))
		},
		&s.wg,
	)
}

func (s *shard) appShardLoadLoop() {
	watchLoop(
		s.ctx,
		s.cr.ew,
		s.cr.ew.nodeAppShardHb(s.service),
		"appShardLoadLoop exit",
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