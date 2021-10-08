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

package smserver

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
	parent *smContainer
	lg     *zap.Logger
	ctx    context.Context
	gs     *apputil.GoroutineStopper

	// 从属于leader或者sm smShard，service和container不一定一样
	service string
}

func newMaintenanceWorker(ctx context.Context, lg *zap.Logger, container *smContainer, service string) *maintenanceWorker {
	w := &maintenanceWorker{
		ctx:     ctx,
		lg:      lg,
		parent:  container,
		service: service,
		gs:      &apputil.GoroutineStopper{},
	}

	w.gs.Wrap(
		func(ctx context.Context) {
			apputil.TickerLoop(
				w.ctx,
				w.lg,
				defaultLoopInterval,
				fmt.Sprintf("[lw] service %s ShardAllocateLoop exit", w.service),
				func(ctx context.Context) error {
					return w.allocateChecker(ctx)
				},
			)
		})

	w.gs.Wrap(
		func(ctx context.Context) {
			apputil.WatchLoop(
				w.ctx,
				w.lg,
				w.parent.Client.Client,
				nodeAppShardHb(w.service),
				fmt.Sprintf("[lw] service %s ShardLoadLoop exit", w.service),
				func(ctx context.Context, ev *clientv3.Event) error {
					return w.shardLoadChecker(ctx, ev)
				},
			)
		})

	w.gs.Wrap(
		func(ctx context.Context) {
			apputil.WatchLoop(
				w.ctx,
				w.lg,
				w.parent.Client.Client,
				nodeAppContainerHb(w.service),
				fmt.Sprintf("[lw] service %s ContainerLoadLoop exit", w.service),
				func(ctx context.Context, ev *clientv3.Event) error {
					return w.containerLoadChecker(ctx, ev)
				},
			)
		})

	w.lg.Info("maintenanceWorker started", zap.String("service", w.service))
	return w
}

func (w *maintenanceWorker) Close() {
	w.gs.Close()
	w.lg.Info("maintenanceWorker stopped", zap.String("service", w.service))
}

// 1 smContainer 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 smShard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func (w *maintenanceWorker) allocateChecker(ctx context.Context) error {
	// 获取当前所有shard
	var (
		etcdFixShardIdAndValue ArmorMap
		err                    error
	)
	shardKey := nodeAppShard(w.service)
	etcdFixShardIdAndValue, err = w.parent.Client.GetKVs(ctx, shardKey)
	if err != nil {
		return errors.Wrap(err, "")
	}
	if len(etcdFixShardIdAndValue) == 0 {
		w.lg.Info("service not init yet",
			zap.String("service", w.service),
			zap.String("node", shardKey),
		)
		return nil
	}
	fixShardIds := etcdFixShardIdAndValue.KeyList()

	// 获取当前存活shard，存活shard的container分配关系如果命中可以不生产moveAction
	etcdSurviveShardIdAndValue, err := w.parent.Client.GetKVs(ctx, nodeAppShardHb(w.service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	currentShardIdAndContainerId := make(ArmorMap)
	for id, value := range etcdSurviveShardIdAndValue {
		var data apputil.ShardHbData
		json.Unmarshal([]byte(value), &data)
		currentShardIdAndContainerId[id] = data.ContainerId
	}
	currentContainerIds := currentShardIdAndContainerId.ValueList()
	currentShardIds := currentShardIdAndContainerId.KeyMap()

	// 现有存活containers
	var surviveContainerIdAndValue ArmorMap
	surviveContainerIdAndValue, err = w.parent.Client.GetKVs(ctx, nodeAppHbContainer(w.service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	surviveContainerIds := surviveContainerIdAndValue.KeyList()

	containerChanged := w.containerChanged(currentContainerIds, surviveContainerIds)
	shardChanged := w.shardChanged(fixShardIds, currentShardIds)
	if containerChanged || shardChanged {
		r := w.reallocate(fixShardIds, surviveContainerIdAndValue, currentShardIdAndContainerId)
		if len(r) > 0 {
			ev := mvEvent{
				Service:     w.service,
				Type:        tContainerUpdate,
				EnqueueTime: time.Now().Unix(),
				Value:       r.String(),
			}
			item := Item{
				Value:    ev.String(),
				Priority: time.Now().Unix(),
			}
			w.parent.eq.push(&item, true)

			w.lg.Info("item enqueue",
				zap.String("service", w.service),
				zap.Reflect("item", item),
			)
		} else {
			// 当survive的container为nil的时候，不能形成有效的分配，直接返回即可
			w.lg.Warn("failed to reallocate",
				zap.Bool("containerChanged", containerChanged),
				zap.Bool("shardChanged", shardChanged),
				zap.String("service", w.service),
				zap.Reflect("survive", surviveContainerIdAndValue),
				zap.Reflect("current", currentShardIdAndContainerId),
			)
		}
	}
	return nil
}

func (w *maintenanceWorker) containerChanged(fixContainerIds []string, surviveContainerIds []string) bool {
	sort.Strings(fixContainerIds)
	sort.Strings(surviveContainerIds)
	return !reflect.DeepEqual(fixContainerIds, surviveContainerIds)
}

func (w *maintenanceWorker) shardChanged(fixShardIds []string, surviveShardIdMap map[string]struct{}) bool {
	for _, fixShardId := range fixShardIds {
		if _, ok := surviveShardIdMap[fixShardId]; !ok {
			return true
		}
	}
	return false
}

func (w *maintenanceWorker) reallocate(fixShardIds []string, surviveContainerIdAndValue ArmorMap, surviveShardIdAndContainerId ArmorMap) moveActionList {
	surviveContainerIds := surviveContainerIdAndValue.KeyList()
	newContainerIdAndShardIds := performAssignment(fixShardIds, surviveContainerIds)

	if len(newContainerIdAndShardIds) > 0 {
		w.lg.Debug("perform assignment start",
			zap.String("service", w.service),
			zap.Strings("fixShardIds", fixShardIds),
			zap.Strings("surviveContainerIds", surviveContainerIds),
			zap.Reflect("expect", newContainerIdAndShardIds),
		)
	}

	// 新增 or 修改，删除操作通过api直接到达被管理服务，停掉后ShardServer不再有hb上传
	var result moveActionList
	for newId, shardIds := range newContainerIdAndShardIds {
		for _, shardId := range shardIds {
			curContainerId, ok := surviveShardIdAndContainerId[shardId]
			if !ok {
				// shard不再任何container内部
				result = append(result, &moveAction{Service: w.service, ShardId: shardId, AddEndpoint: newId})
				continue
			}

			// shardId没有被分配到container，不对这个shard做任何处理，应该是程序bug
			if curContainerId == "" {
				w.lg.Error("got empty container id in shard hb node",
					zap.String("service", w.service),
					zap.String("shardId", shardId),
				)
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

			result = append(result, &moveAction{Service: w.service, ShardId: shardId, DropEndpoint: curContainerId, AddEndpoint: newId, AllowDrop: allowDrop})
		}
	}

	if len(result) > 0 {
		w.lg.Info("perform assignment complete",
			zap.String("service", w.service),
			zap.Reflect("newContainerIdAndShardIds", newContainerIdAndShardIds),
			zap.Reflect("surviveShardIdAndContainerId", surviveShardIdAndContainerId),
			zap.Reflect("result", result),
		)
	}
	return result
}

func (w *maintenanceWorker) shardLoadChecker(_ context.Context, ev *clientv3.Event) error {
	if ev.IsCreate() {
		return nil
	}

	start := time.Now()
	qev := mvEvent{
		Service:     w.service,
		Type:        tShardUpdate,
		EnqueueTime: start.Unix(),
		Value:       string(ev.Kv.Value),
	}

	var item Item
	if ev.IsModify() {
		qev.Type = tShardUpdate
	} else {
		qev.Type = tShardDel

		// 3s是给服务器container重启的时间buffer
		item.Priority = start.Add(3 * time.Second).Unix()
	}
	item.Value = qev.String()

	w.parent.eq.push(&item, true)
	return nil
}

func (w *maintenanceWorker) containerLoadChecker(_ context.Context, ev *clientv3.Event) error {
	if ev.IsCreate() {
		return nil
	}

	start := time.Now()
	qev := mvEvent{
		Service:     w.service,
		Type:        tContainerUpdate,
		EnqueueTime: start.Unix(),
		Value:       string(ev.Kv.Value),
	}

	var item Item
	if ev.IsModify() {
		qev.Type = tContainerUpdate
	} else {
		qev.Type = tContainerDel
		// 3s是给服务器container重启的事件
		item.Priority = start.Add(3 * time.Second).Unix()
	}
	item.Value = qev.String()

	w.parent.eq.push(&item, true)
	return nil
}
