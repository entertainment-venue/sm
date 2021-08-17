package main

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

type shard struct {
	id      string
	service string

	ew *etcdWrapper

	eq *eventQueue
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

		k := s.ew.heartbeatShardIdNode(s.id)
		// TODO 增加shard负载到val中，方便leader分析
		if _, err := s.ew.etcdClientV3.Put(ctx, k, "", clientv3.WithLease(session.Lease())); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}
	}
}

func (s *shard) ownerLoop() {

}

func (s *shard) shardLoadLoop() {

}

func (s *shard) containerLoadLoop() {

}
