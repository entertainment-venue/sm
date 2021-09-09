package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

// 抽离出的目的是发现leader和普通的sm shard都具备相似的能力，代码基本一致
type MaintenanceWorker interface {
	Starter
	Closer

	// 监控sm集群本身以及各接入业务app的shard负载
	ShardLoadLoop()

	// 负载的另一个维度，container，相比shard本身的load，在初期更容易理解，也更容易构建
	ContainerLoadLoop()

	// 监控sm集群本身以及各接入业务app的shard分配是否合理
	ShardAllocateLoop()
}

// 管理某个sm app的shard
type maintenanceWorker struct {
	goroutineStopper

	service string

	ctr *container
}

func newMaintenanceWorker(ctr *container, service string) *maintenanceWorker {
	var mw maintenanceWorker
	mw.ctx, mw.cancel = context.WithCancel(context.Background())
	mw.ctr = ctr
	mw.service = service
	return &mw
}

func (w *maintenanceWorker) Start() {
	w.wg.Add(3)
	go w.ShardAllocateLoop()
	go w.ShardLoadLoop()
	go w.ContainerLoadLoop()
}

func (w *maintenanceWorker) Close() {
	w.cancel()
	w.wg.Wait()
	Logger.Printf("maintenanceWorker for service %s stopped", w.ctr.service)
}

func (w *maintenanceWorker) ShardAllocateLoop() {
	tickerLoop(
		w.ctx,
		defaultShardLoopInterval,
		fmt.Sprintf("[mw] service %s ShardAllocateLoop exit", w.service),
		func(ctx context.Context) error {
			return allocateChecker(ctx, w.ctr.ew, w.service, w.ctr.eq)
		},
		&w.wg,
	)
}

func (w *maintenanceWorker) ShardLoadLoop() {
	watchLoop(
		w.ctx,
		w.ctr.ew,
		w.ctr.ew.nodeAppShardHb(w.service),
		fmt.Sprintf("[mw] service %s ShardLoadLoop exit", w.service),
		func(ctx context.Context, ev *clientv3.Event) error {
			return shardLoadChecker(ctx, w.service, w.ctr.eq, ev)
		},
		&w.wg,
	)
}

func (w *maintenanceWorker) ContainerLoadLoop() {
	watchLoop(
		w.ctx,
		w.ctr.ew,
		w.ctr.ew.nodeAppContainerHb(w.service),
		fmt.Sprintf("[mw] service %s ContainerLoadLoop exit", w.service),
		func(ctx context.Context, ev *clientv3.Event) error {
			return containerLoadChecker(ctx, w.service, w.ctr.eq, ev)
		},
		&w.wg,
	)

}

// 1 container 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 shard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func allocateChecker(ctx context.Context, ew *etcdWrapper, service string, eq *eventQueue) error {
	// 获取当前的shard分配关系
	fixShardIdAndValue, err := ew.EtcdClient.GetKVs(ctx, ew.nodeAppShard(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	// 检查是否有shard没有在健康的container上
	fixShardIdAndContainerId := make(ArmorMap)
	for id, value := range fixShardIdAndValue {
		var ss shardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}
		fixShardIdAndContainerId[id] = ss.ContainerId
	}

	// 现有存活containers
	var surviveContainerIdAndValue ArmorMap
	surviveContainerIdAndValue, err = ew.EtcdClient.GetKVs(ctx, ew.nodeAppHbContainer(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	if containerChanged(fixShardIdAndContainerId.ValueList(), surviveContainerIdAndValue.KeyList()) {
		r := reallocate(service, surviveContainerIdAndValue, fixShardIdAndContainerId)
		if len(r) > 0 {
			Logger.Printf("[utils] service %s containerChanged true", service)

			item := Item{
				Value:    r.String(),
				Priority: time.Now().Unix(),
			}
			eq.push(&item, true)

			Logger.Printf("Container changed for service %s, enqueue item %s", service, item.String())
			return nil
		}
	}
	Logger.Printf("[utils] service %s containerChanged false", service)

	// container hb和固定分配关系一致，下面检查shard存活
	var surviveShardIdAndValue ArmorMap
	surviveShardIdAndValue, err = ew.EtcdClient.GetKVs(ctx, ew.nodeAppShardHb(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	if shardChanged(fixShardIdAndContainerId.KeyList(), surviveShardIdAndValue.KeyMap()) {
		r := reallocate(service, surviveContainerIdAndValue, fixShardIdAndContainerId)
		if len(r) > 0 {
			Logger.Printf("[utils] service %s shardChanged true", service)

			item := Item{
				Value:    r.String(),
				Priority: time.Now().Unix(),
			}
			eq.push(&item, true)

			Logger.Printf("Container changed for service %s, result %v", service, r)
		}
	}
	Logger.Printf("[utils] service %s shardChanged false", service)

	return nil
}

func containerChanged(fixContainerIds []string, surviveContainerIds []string) bool {
	sort.Strings(fixContainerIds)
	sort.Strings(surviveContainerIds)
	return !reflect.DeepEqual(fixContainerIds, surviveContainerIds)
}

func shardChanged(fixShardIds []string, surviveShardIdMap map[string]struct{}) bool {
	for _, fixShardId := range fixShardIds {
		if _, ok := surviveShardIdMap[fixShardId]; !ok {
			return true
		}
	}
	return false
}

func reallocate(service string, surviveContainerIdAndValue ArmorMap, fixShardIdAndContainerId ArmorMap) moveActionList {
	shardIds := fixShardIdAndContainerId.KeyList()
	surviveContainerIds := surviveContainerIdAndValue.KeyList()
	newContainerIdAndShardIds := performAssignment(shardIds, surviveContainerIds)

	var result moveActionList
	for newId, shardIds := range newContainerIdAndShardIds {
		for _, shardId := range shardIds {
			curContainerId := fixShardIdAndContainerId[shardId]

			// shardId没有被分配到container，可以直接增加moveAction
			if curContainerId == "" {
				result = append(result, &moveAction{Service: service, ShardId: shardId, AddEndpoint: newId})
				continue
			}

			// shardId当前的container符合最新的分配结果，不需要shard move
			if curContainerId == newId {
				continue
			}

			// curContainerId 可能不存在与containerIdAndValue，你给他发drop，可能也无法处理，判断是否是survive的container，不是，允许drop
			var allowDrop bool
			if _, ok := surviveContainerIdAndValue[curContainerId]; !ok {
				allowDrop = true
			}

			result = append(result, &moveAction{Service: service, ShardId: shardId, DropEndpoint: curContainerId, AddEndpoint: newId, AllowDrop: allowDrop})
		}
	}
	return result
}

func shardLoadChecker(_ context.Context, service string, eq *eventQueue, ev *clientv3.Event) error {
	if ev.IsCreate() {
		return nil
	}

	start := time.Now()
	qev := loadEvent{Service: service, EnqueueTime: start.Unix(), Load: string(ev.Kv.Value)}

	var item Item
	if ev.IsModify() {
		qev.Type = evTypeShardUpdate
	} else {
		qev.Type = evTypeShardDel

		// 3s是给服务器container重启的时间buffer
		item.Priority = start.Add(3 * time.Second).Unix()
	}
	item.Value = qev.String()

	eq.push(&item, true)
	return nil
}

func containerLoadChecker(_ context.Context, service string, eq *eventQueue, ev *clientv3.Event) error {
	if ev.IsCreate() {
		return nil
	}

	start := time.Now()
	qev := loadEvent{Service: service, EnqueueTime: start.Unix(), Load: string(ev.Kv.Value)}

	var item Item
	if ev.IsModify() {
		qev.Type = evTypeContainerUpdate
	} else {
		qev.Type = evTypeContainerDel
		// 3s是给服务器container重启的事件
		item.Priority = start.Add(3 * time.Second).Unix()
	}
	item.Value = qev.String()

	eq.push(&item, true)
	return nil
}
