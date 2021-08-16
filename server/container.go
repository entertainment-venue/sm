package main

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
)

type leader struct {
	container
}

type container struct {
	id          string
	application string

	shards []*shard

	etcdWrapper *etcdWrapper
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

		election := concurrency.NewElection(session, c.etcdWrapper.leaderNode())
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

func (c *leader) checkShardOwnerLoop(ctx context.Context) {
	ticker := time.Tick(3 * time.Second)
	for {
	checkLoop:
		select {
		case <-ctx.Done():
			Logger.Printf("campaignLeader exit")
			return
		case <-ticker:
			containerIds, err := c.etcdWrapper.getKvs(ctx, c.etcdWrapper.heartbeatContainerNode())
			if err != nil {
				Logger.Printf("err %+v", err)
				goto checkLoop
			}

			shardIds, err := c.etcdWrapper.getKvs(ctx, c.etcdWrapper.heartbeatShardNode())
			if err != nil {
				Logger.Printf("err %+v", err)
				goto checkLoop
			}

			for _, v := range shardIds {
				var shb ShardHeartbeat
				if err := json.Unmarshal([]byte(v), &shb); err != nil {
					Logger.Printf("err %+v", err)
					goto checkLoop
				}
				if _, ok := containerIds[shb.ContainerId]; !ok {
					// TODO 触发一次任务分配
				}
			}
		}
	}
}

func (c *leader) watchShardLoadLoop(ctx context.Context) {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())

watchLoop:
	wch := c.etcdWrapper.etcdClientV3.Watch(ctx, c.etcdWrapper.heartbeatShardNode())
	for {
		select {
		case <-ctx.Done():
			Logger.Printf("watchShardLoadLoop exit")
			return
		case wr := <-wch:
			if err := wr.Err(); err != nil {
				Logger.Printf("err: %v", err)
				goto watchLoop
			}

			for _, ev := range wr.Events {
				if ev.IsCreate() {
					continue
				}

				if ev.IsModify() {
					// TODO 利用prevkv和kv可以得到load是否改变，或者改变的阈值是否超出spec中定义的阈值，load变化很正常
				} else {
					// TODO 删除事件同上，会给到一个goroutine池子处理
				}
			}
		}
	}
}

func (c *leader) watchContainerLoadLoop(ctx context.Context) {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())

watchLoop:
	wch := c.etcdWrapper.etcdClientV3.Watch(ctx, c.etcdWrapper.heartbeatContainerNode())
	for {
		select {
		case <-ctx.Done():
			Logger.Printf("watchContainerLoadLoop exit")
			return
		case wr := <-wch:
			if err := wr.Err(); err != nil {
				Logger.Printf("err: %v", err)
				goto watchLoop
			}

			for _, ev := range wr.Events {
				if ev.IsCreate() {
					continue
				}

				if ev.IsModify() {
					// TODO 利用prevkv和kv可以得到load是否改变，或者改变的阈值是否超出spec中定义的阈值，load变化很正常
				} else {
					// TODO 删除事件同上，会给到一个goroutine池子处理
				}
			}
		}
	}
}
