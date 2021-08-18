package main

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

type container struct {
	id          string
	application string

	shards []*shard

	ew *etcdWrapper
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

type leader struct {
	container

	eq *eventQueue
}

func newLeader(ctx context.Context, cr *container) *leader {
	return &leader{
		eq: newEventQueue(ctx),
	}
}

func (l *leader) shardOwnerLoop(ctx context.Context) {
	waitTickerLoop(
		ctx,
		defaultShardLoopInterval,
		"shardOwnerLoop exit",
		func(ctx context.Context) error {
			return checkShardOwner(
				ctx,
				l.ew,
				l.ew.heartbeatContainerNode(true),
				l.ew.heartbeatShardNode(true))
		},
	)
}

func (l *leader) shardLoadLoop(ctx context.Context) {
	waitWatchLoop(
		ctx,
		l.ew,
		l.ew.heartbeatShardNode(true),
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
			l.eq.push(&qev)
			return nil
		},
	)
}

func (l *leader) containerLoadLoop(ctx context.Context) {
	waitWatchLoop(
		ctx,
		l.ew,
		l.ew.heartbeatContainerNode(true),
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
