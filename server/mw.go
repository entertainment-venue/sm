// Copyright 2021 The entertainment-venue Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
)

// 管理某个sm app的shard
type maintenanceWorker struct {
	parent *serverContainer

	ctx context.Context

	stopper *apputil.GoroutineStopper

	// 从属于leader或者sm serverShard，service和container不一定一样
	service string
}

func newMaintenanceWorker(container *serverContainer, service string) *maintenanceWorker {
	return &maintenanceWorker{parent: container, service: service, stopper: &apputil.GoroutineStopper{}}
}

func (w *maintenanceWorker) Start() {
	w.stopper.Wrap(
		func(ctx context.Context) {
			apputil.TickerLoop(
				w.ctx,
				defaultShardLoopInterval,
				fmt.Sprintf("[mtWorker] service %s ShardAllocateLoop exit", w.service),
				func(ctx context.Context) error {
					return w.allocateChecker(ctx, w.parent.ew, w.service, w.parent.eq)
				},
			)
		})

	w.stopper.Wrap(
		func(ctx context.Context) {
			apputil.WatchLoop(
				w.ctx,
				w.parent.Client.Client,
				w.parent.ew.nodeAppShardHb(w.service),
				fmt.Sprintf("[mtWorker] service %s ShardLoadLoop exit", w.service),
				func(ctx context.Context, ev *clientv3.Event) error {
					return shardLoadChecker(ctx, w.service, w.parent.eq, ev)
				},
			)
		})

	w.stopper.Wrap(
		func(ctx context.Context) {
			apputil.WatchLoop(
				w.ctx,
				w.parent.Client.Client,
				w.parent.ew.nodeAppContainerHb(w.service),
				fmt.Sprintf("[mtWorker] service %s ContainerLoadLoop exit", w.service),
				func(ctx context.Context, ev *clientv3.Event) error {
					return containerLoadChecker(ctx, w.service, w.parent.eq, ev)
				},
			)
		})
}

func (w *maintenanceWorker) Close() {
	w.stopper.Close()
	Logger.Printf("maintenanceWorker for service %s stopped", w.parent.service)
}

// 1 serverContainer 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 serverShard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func (w *maintenanceWorker) allocateChecker(ctx context.Context, ew *etcdWrapper, service string, eq *eventQueue) error {
	// 获取当前的shard分配关系
	fixShardIdAndValue, err := w.parent.Client.GetKVs(ctx, ew.nodeAppShard(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	// 检查是否有shard没有在健康的container上
	fixShardIdAndContainerId := make(ArmorMap)
	for id, value := range fixShardIdAndValue {
		var ss apputil.ShardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}
		fixShardIdAndContainerId[id] = ss.ContainerId
	}

	// 现有存活containers
	var surviveContainerIdAndValue ArmorMap
	surviveContainerIdAndValue, err = w.parent.Client.GetKVs(ctx, ew.nodeAppHbContainer(service))
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

	// serverContainer hb和固定分配关系一致，下面检查shard存活
	var surviveShardIdAndValue ArmorMap
	surviveShardIdAndValue, err = w.parent.Client.GetKVs(ctx, ew.nodeAppShardHb(service))
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
		qev.Type = tShardUpdate
	} else {
		qev.Type = tShardDel

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
		qev.Type = tContainerUpdate
	} else {
		qev.Type = tContainerDel
		// 3s是给服务器container重启的事件
		item.Priority = start.Add(3 * time.Second).Unix()
	}
	item.Value = qev.String()

	eq.push(&item, true)
	return nil
}
