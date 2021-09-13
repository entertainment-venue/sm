package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/entertainment-venue/borderland/pkg/apputil"
	"github.com/pkg/errors"
)

type leaderEtcdValue struct {
	ContainerId string `json:"containerId"`
	CreateTime  string `json:"createTime"`
}

func (v *leaderEtcdValue) String() string {
	b, _ := json.Marshal(v)
	return string(b)
}

type leader struct {
	parent *serverContainer

	ctx context.Context

	stopper *apputil.GoroutineStopper

	// 保证borderland运行健康的goroutine，通过task节点下发任务给op
	mtWorker *maintenanceWorker

	// op需要监听特定app的task在etcd中的节点，保证app级别只有一个，borderland放在leader中
	op *operator
}

func newLeader(ctx context.Context, container *serverContainer) *leader {
	l := leader{ctx: ctx, parent: container}

	l.mtWorker = newMaintenanceWorker(container, container.service)

	l.stopper.Wrap(
		func(ctx context.Context) {
			l.campaign()
		})

	return &l
}

func (l *leader) campaign() {
	for {
	loop:
		select {
		case <-l.ctx.Done():
			Logger.Printf("[leader] service %s campaign exit", l.parent.service)
			return
		default:
		}

		leaderNodePrefix := l.parent.ew.leaderNode(l.parent.service)
		lvalue := leaderEtcdValue{ContainerId: l.parent.id, CreateTime: time.Now().String()}
		election := concurrency.NewElection(l.parent.Session, leaderNodePrefix)
		if err := election.Campaign(l.ctx, lvalue.String()); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}
		Logger.Printf("[leader] service %s Successfully campaign for current serverContainer %s with leader %s/%d", l.parent.service, l.parent.id, leaderNodePrefix, l.parent.Session.Lease())

		// leader启动时，等待一个时间段，方便所有container做至少一次heartbeat，然后开始监测是否需要进行container和shard映射关系的变更。
		// etcd sdk中keepalive的请求发送时间时500ms，3s>>500ms，认为这个时间段内，所有container都会发heartbeat，不存在的就认为没有任务。
		time.Sleep(5 * time.Second)

		if err := l.init(); err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		// leader需要处理shard move的任务
		var err error
		l.op, err = newOperator(l.parent, l.parent.service)
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto loop
		}

		// 检查所有shard应该都被分配container，当前app的配置信息是预先录入etcd的。此时提取该信息，得到所有shard的id，
		// https://github.com/entertainment-venue/borderland/wiki/leader%E8%AE%BE%E8%AE%A1%E6%80%9D%E8%B7%AF
		go l.mtWorker.Start()

		// block until出现需要放弃leader职权的事件
		Logger.Printf("[leader] service %s completed start operator and mtWorker, block until exit", l.parent.service)
		select {
		case <-l.ctx.Done():
			Logger.Printf("[leader] service %s campaign exit", l.parent.service)
			return
		}
	}
}

func (l *leader) init() error {
	// 先把当前的分配关系下发下去，和static membership，不过我们场景是由单点完成的，由性能瓶颈，但是不像LRMF场景下serverless难以判断正确性
	// 分配关系下发，解决的是先把现有分配关系搞下去，然后再通过shardAllocateLoop检验是否需要整体进行shard move，相当于init
	// TODO app接入数量一个公司可控，所以方案可行
	bdShardNode := l.parent.ew.nodeAppShard(l.parent.service)
	curShardIdAndValue, err := l.parent.Client.GetKVs(l.ctx, bdShardNode)
	if err != nil {
		return errors.Wrap(err, "")
	}
	var moveActions moveActionList
	for shardId, value := range curShardIdAndValue {
		var ss shardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}

		// 未分配container的shard，不需要move指令下发
		if ss.ContainerId != "" {
			// 下发指令，接受不了的直接干掉当前的分配关系
			ma := moveAction{Service: l.parent.service, ShardId: shardId, AddEndpoint: ss.ContainerId, AllowDrop: true}
			moveActions = append(moveActions, &ma)
			Logger.Printf("[leader] service %s Init move action %+v", ma, l.parent.service)
		}
	}
	// 向自己的app任务节点发任务
	if len(moveActions) == 0 {
		Logger.Printf("[leader] service %s No move action created", l.parent.service)
		return nil
	}
	bdTaskNode := l.parent.ew.nodeAppTask(l.parent.service)
	if _, err := l.parent.Client.CompareAndSwap(l.ctx, bdTaskNode, "", moveActions.String(), -1); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (l *leader) close() {
	if l.op != nil {
		l.op.Close()
	}
	Logger.Printf("[leader] stopped for service %s ", l.parent.service)
}
