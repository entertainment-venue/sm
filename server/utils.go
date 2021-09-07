package server

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

// 提出container和shard的公共属性
// 抽象数据结构，也会引入数据结构之间的耦合
type goroutineStopper struct {
	// https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
	// goroutineStopper close
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func tickerLoop(ctx context.Context, duration time.Duration, exitMsg string, fn func(ctx context.Context) error, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.Tick(duration)
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			Logger.Printf(exitMsg)
			return
		}
		if err := fn(ctx); err != nil {
			Logger.Printf("err: %v", err)
		}
	}
}

func watchLoop(ctx context.Context, ew *etcdWrapper, node string, exitMsg string, fn func(ctx context.Context, ev *clientv3.Event) error, wg *sync.WaitGroup) {
	defer wg.Done()

	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())

watchLoop:
	wch := ew.client.Watch(ctx, node)
	for {
		var wr clientv3.WatchResponse
		select {
		case wr = <-wch:
		case <-ctx.Done():
			Logger.Printf(exitMsg)
			return
		}
		if err := wr.Err(); err != nil {
			Logger.Printf("err: %v", err)
			goto watchLoop
		}

		for _, ev := range wr.Events {
			if err := fn(ctx, ev); err != nil {
				Logger.Printf("err: %v", err)
				time.Sleep(defaultSleepTimeout)
				goto watchLoop
			}
		}
	}
}

// 1 container 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 shard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func allocateChecker(ctx context.Context, ew *etcdWrapper, service string) error {
	// 获取当前的shard分配关系
	fixShardIdAndValue, err := ew.getKvs(ctx, ew.nodeAppShard(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	// 检查是否有shard没有在健康的container上
	var (
		fixShardIds              []string
		fixContainerIds          []string
		fixContainerIdMap        = make(map[string]struct{})
		fixShardIdAndContainerId = make(map[string]string)

		actions moveActionList
	)
	for id, value := range fixShardIdAndValue {
		fixShardIds = append(fixShardIds, id)

		var ss shardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}

		fixContainerIds = append(fixContainerIds, ss.ContainerId)
		fixContainerIdMap[ss.ContainerId] = struct{}{}
		fixShardIdAndContainerId[id] = ss.ContainerId
	}

	// 现有存活containers
	surviveContainerIdAndValue, err := ew.getKvs(ctx, ew.nodeAppHbContainer(service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	var surviveContainerIds []string
	for containerId := range surviveContainerIdAndValue {
		surviveContainerIds = append(surviveContainerIds, containerId)
	}

	var containerChanged bool
	sort.Strings(fixContainerIds)
	sort.Strings(surviveContainerIds)
	if !reflect.DeepEqual(fixContainerIds, surviveContainerIds) {
		containerChanged = true
	}
	if containerChanged {
		r := reallocate(service, surviveContainerIdAndValue, fixShardIdAndContainerId)
		actions = append(actions, r...)
		if len(actions) > 0 {
			// 向自己的app任务节点发任务
			if _, err := ew.compareAndSwap(ctx, ew.nodeAppTask(service), "", actions.String(), -1); err != nil {
				return errors.Wrap(err, "")
			}
			Logger.Printf("Container changed for service %s, result %s", service, actions.String())
			return nil
		}
	}

	// container hb和固定分配关系一致，下面检查shard存活
	var shardChanged bool
	surviveShardIdAndValue, err := ew.getKvs(ctx, ew.nodeAppShardHb(service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	surviveShardIdMap := make(map[string]struct{})
	for id := range surviveShardIdAndValue {
		surviveShardIdMap[id] = struct{}{}
	}
	for _, fixShardId := range fixShardIds {
		if _, ok := surviveShardIdMap[fixShardId]; !ok {
			shardChanged = true
			break
		}
	}

	if shardChanged {
		r := reallocate(service, surviveContainerIdAndValue, fixShardIdAndContainerId)
		actions = append(actions, r...)
		if len(actions) > 0 {
			// 向自己的app任务节点发任务
			if _, err := ew.compareAndSwap(ctx, ew.nodeAppTask(service), "", actions.String(), -1); err != nil {
				return errors.Wrap(err, "")
			}
			Logger.Printf("Container changed for service %s, result %v", service, actions)
		}
	}

	return nil
}

func reallocate(service string, surviveContainerIdAndValue map[string]string, fixShardIdAndContainerId map[string]string) moveActionList {
	var shardIds []string
	for shardId := range fixShardIdAndContainerId {
		shardIds = append(shardIds, shardId)
	}

	// survive的map有两个作用：
	// 1. 为分配方法performAssignment提供基础数据
	// 2. 非存活状态的container如果
	var surviveContainerIds []string
	for containerId := range surviveContainerIdAndValue {
		surviveContainerIds = append(surviveContainerIds, containerId)
	}
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
