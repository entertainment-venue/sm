package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

type borderlandLeader struct {
	admin

	ctr *container

	mw MaintenanceWorker
}

func newBorderlandLeader(ctr *container) *borderlandLeader {
	var leader borderlandLeader
	leader.ctx, leader.cancel = context.WithCancel(context.Background())
	leader.ctr = ctr
	leader.mw = newMaintenanceWorker(ctr)

	leader.wg.Add(1)
	go leader.campaign()

	return &leader
}

func (leader *borderlandLeader) campaign() {
	for {
	loop:
		select {
		case <-leader.ctx.Done():
			Logger.Printf("campaign exit")
			return
		default:
		}

		election := concurrency.NewElection(leader.ctr.session, leader.ctr.ew.leaderNode())
		if err := election.Campaign(leader.ctx, leader.id); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		Logger.Printf("Successfully campaign for current ctr %s", leader.id)

		// leader启动时，等待一个时间段，方便所有container做至少一次heartbeat，然后开始监测是否需要进行container和shard映射关系的变更。
		// etcd sdk中keepalive的请求发送时间时500ms，3s>>500ms，认为这个时间段内，所有container都会发heartbeat，不存在的就认为没有任务。
		time.Sleep(15 * time.Second)

		// 先把当前的分配关系下发下去，和static membership，不过我们场景是由单点完成的，由性能瓶颈，但是不像LRMF场景下serverless难以判断正确性
		// 分配关系下发，解决的是先把现有分配关系搞下去，然后再通过shardAllocateLoop检验是否需要整体进行shard move，相当于init
		// TODO app接入数量一个公司可控，所以方案可行
		shardIdAndValue, err := leader.ctr.ew.getKvs(leader.ctx, leader.ctr.ew.nodeAppShard(leader.service))
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
			moveActions = append(moveActions, &moveAction{Service: leader.service, ShardId: shardId, AddEndpoint: ss.ContainerId, AllowDrop: true})
		}
		// 向自己的app任务节点发任务
		if _, err := leader.ctr.ew.compareAndSwap(leader.ctx, leader.ctr.ew.nodeAppTask(leader.service), "", moveActions.String(), -1); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		// 检查所有shard应该都被分配container，当前app的配置信息是预先录入etcd的。此时提取该信息，得到所有shard的id，
		// https://github.com/entertainment-venue/borderland/wiki/leader%E8%AE%BE%E8%AE%A1%E6%80%9D%E8%B7%AF
		go leader.mw.ContainerLoadLoop()
	}
}

func (leader *borderlandLeader) Close() {
	leader.cancel()
	leader.wg.Wait()
	Logger.Printf("leader for service %s stopped", leader.ctr.service)
}

func (leader *borderlandLeader) ShardAllocateLoop() {
	tickerLoop(
		leader.ctx,
		defaultShardLoopInterval,
		"shardAllocateLoop exit",
		func(ctx context.Context) error {
			return shardAllocateChecker(ctx, leader.ctr.ew, leader.service)
		},
		&leader.wg,
	)
}

func (leader *borderlandLeader) ShardLoadLoop() {
	watchLoop(
		leader.ctx,
		leader.ctr.ew,
		leader.ctr.ew.nodeAppShardHb(leader.service),
		"shardLoadLoop exit",
		func(ctx context.Context, ev *clientv3.Event) error {
			return shardLoadChecker(ctx, leader.ctr.eq, ev)
		},
		&leader.wg,
	)
}

func (leader *borderlandLeader) ContainerLoadLoop() {
	watchLoop(
		leader.ctx,
		leader.ctr.ew,
		leader.ctr.ew.nodeAppContainerHb(leader.service),
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
			leader.ctr.eq.push(&qev)
			return nil
		},
		&leader.wg,
	)
}
