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
	"go.uber.org/zap"
)

// 管理某个sm app的shard
type maintenanceWorker struct {
	parent *serverContainer

	ctx context.Context

	stopper *apputil.GoroutineStopper

	// 从属于leader或者sm serverShard，service和container不一定一样
	service string

	lg *zap.Logger
}

func newMaintenanceWorker(ctx context.Context, container *serverContainer, service string) *maintenanceWorker {
	return &maintenanceWorker{parent: container, service: service, stopper: &apputil.GoroutineStopper{}}
}

func (w *maintenanceWorker) Start() {
	w.stopper.Wrap(
		func(ctx context.Context) {
			apputil.TickerLoop(
				w.ctx,
				w.lg,
				defaultLoopInterval,
				fmt.Sprintf("[mtWorker] service %s ShardAllocateLoop exit", w.service),
				func(ctx context.Context) error {
					return w.allocateChecker(ctx, w.service, w.parent.eq)
				},
			)
		})

	w.stopper.Wrap(
		func(ctx context.Context) {
			apputil.WatchLoop(
				w.ctx,
				w.lg,
				w.parent.Client.Client,
				nodeAppShardHb(w.service),
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
				w.lg,
				w.parent.Client.Client,
				nodeAppContainerHb(w.service),
				fmt.Sprintf("[mtWorker] service %s ContainerLoadLoop exit", w.service),
				func(ctx context.Context, ev *clientv3.Event) error {
					return containerLoadChecker(ctx, w.service, w.parent.eq, ev)
				},
			)
		})
}

func (w *maintenanceWorker) Close() {
	w.stopper.Close()
	w.lg.Info("maintenanceWorker stopped", zap.String("service", w.service))
}

// 1 serverContainer 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 serverShard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func (w *maintenanceWorker) allocateChecker(ctx context.Context, service string, eq *eventQueue) error {
	// 获取当前的shard分配关系
	fixShardIdAndValue, err := w.parent.Client.GetKVs(ctx, nodeAppShard(service))
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
	surviveContainerIdAndValue, err = w.parent.Client.GetKVs(ctx, nodeAppHbContainer(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	if containerChanged(fixShardIdAndContainerId.ValueList(), surviveContainerIdAndValue.KeyList()) {
		r := w.reallocate(service, surviveContainerIdAndValue, fixShardIdAndContainerId)
		if len(r) > 0 {
			ev := mvEvent{Service: service, EnqueueTime: time.Now().Unix(), Value: r.String()}
			item := Item{
				Value:    ev.String(),
				Priority: time.Now().Unix(),
			}
			eq.push(&item, true)

			w.lg.Info("container changed cause item enqueue",
				zap.String("service", service),
				zap.String("item", item.String()),
			)
			return nil
		}
	}
	w.lg.Debug("container not changed", zap.String("service", service))

	// serverContainer hb和固定分配关系一致，下面检查shard存活
	var surviveShardIdAndValue ArmorMap
	surviveShardIdAndValue, err = w.parent.Client.GetKVs(ctx, nodeAppShardHb(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	if shardChanged(fixShardIdAndContainerId.KeyList(), surviveShardIdAndValue.KeyMap()) {
		r := w.reallocate(service, surviveContainerIdAndValue, fixShardIdAndContainerId)
		if len(r) > 0 {
			ev := mvEvent{Service: service, EnqueueTime: time.Now().Unix(), Value: r.String()}
			item := Item{
				Value:    ev.String(),
				Priority: time.Now().Unix(),
			}
			eq.push(&item, true)

			w.lg.Info("shard changed cause item enqueue",
				zap.String("service", service),
				zap.String("item", item.String()),
			)

			return nil
		}
	}
	w.lg.Debug("shard not changed", zap.String("service", service))

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

func (w *maintenanceWorker) reallocate(service string, surviveContainerIdAndValue ArmorMap, fixShardIdAndContainerId ArmorMap) moveActionList {
	shardIds := fixShardIdAndContainerId.KeyList()
	surviveContainerIds := surviveContainerIdAndValue.KeyList()
	newContainerIdAndShardIds := performAssignment(shardIds, surviveContainerIds)

	w.lg.Info("performAssignment",
		zap.Strings("shardIds", shardIds),
		zap.Strings("surviveContainerIds", surviveContainerIds),
		zap.Reflect("result", newContainerIdAndShardIds),
	)

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
	qev := mvEvent{Service: service, EnqueueTime: start.Unix(), Value: string(ev.Kv.Value)}

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
	qev := mvEvent{Service: service, EnqueueTime: start.Unix(), Value: string(ev.Kv.Value)}

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
