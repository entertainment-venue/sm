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
		etcdShardIdAndAny ArmorMap
		err               error
	)
	shardKey := nodeAppShard(w.service)
	etcdShardIdAndAny, err = w.parent.Client.GetKVs(ctx, shardKey)
	if err != nil {
		return errors.Wrap(err, "")
	}
	if len(etcdShardIdAndAny) == 0 {
		w.lg.Info("service not init yet",
			zap.String("service", w.service),
			zap.String("node", shardKey),
		)
		return nil
	}
	// 支持手动指定container
	fixShardIdAndManualContainerId := make(ArmorMap)
	for id, value := range etcdShardIdAndAny {
		var ss apputil.ShardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}
		fixShardIdAndManualContainerId[id] = ss.ManualContainerId
	}

	// 获取当前存活shard，存活shard的container分配关系如果命中可以不生产moveAction
	etcdHbShardIdAndValue, err := w.parent.Client.GetKVs(ctx, nodeAppShardHb(w.service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	hbShardIdAndContainerId := make(ArmorMap)
	for id, value := range etcdHbShardIdAndValue {
		var data apputil.ShardHbData
		if err := json.Unmarshal([]byte(value), &data); err != nil {
			return errors.Wrap(err, "")
		}
		hbShardIdAndContainerId[id] = data.ContainerId
	}

	// 现有存活containers
	var etcdHbContainerIdAndAny ArmorMap
	etcdHbContainerIdAndAny, err = w.parent.Client.GetKVs(ctx, nodeAppHbContainer(w.service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	containerChanged := w.changed(hbShardIdAndContainerId.ValueList(), etcdHbContainerIdAndAny.KeyList())
	shardChanged := w.changed(etcdShardIdAndAny.KeyList(), hbShardIdAndContainerId.KeyList())
	if containerChanged || shardChanged {
		r := w.reallocate(fixShardIdAndManualContainerId, etcdHbContainerIdAndAny, hbShardIdAndContainerId)
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
				zap.Bool("container-changed", containerChanged),
				zap.Bool("shard-changed", shardChanged),
				zap.String("service", w.service),
				zap.Reflect("shardIdAndManualContainerId", fixShardIdAndManualContainerId),
				zap.Reflect("etcdHbContainerIdAndAny", etcdHbContainerIdAndAny),
				zap.Reflect("hbShardIdAndContainerId", hbShardIdAndContainerId),
			)
		}
	}
	return nil
}

func (w *maintenanceWorker) changed(a []string, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	return !reflect.DeepEqual(a, b)
}

type containerWeight struct {
	id     string
	shards []string
}

type containerWeightList []*containerWeight

func (l containerWeightList) Len() int { return len(l) }
func (l containerWeightList) Less(i, j int) bool {
	return l[i].id < l[j].id
}
func (l containerWeightList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l containerWeightList) IsConflict(shards []string) bool {
	r := make(map[string]struct{}, len(l))
	for _, cw := range l {
		for _, id := range cw.shards {
			r[id] = struct{}{}
		}
	}
	for _, shard := range shards {
		if _, ok := r[shard]; ok {
			return true
		}
	}
	return false
}

// extractNeedAssignShardIds 得到哪些shardId需要更换位置
// 1 fixShardIds中不存活的需要
// 2 fixShardIds中存活，但是位置不对的需要（位置和fixShardIdAndManualContainerId中的位置不匹配）
func (w *maintenanceWorker) extractNeedAssignShardIds(shardIdAndManualContainerId ArmorMap, surviveContainerIdAndAny ArmorMap, hbShardIdAndContainerId ArmorMap) ([]string, moveActionList) {
	var (
		resultMAL moveActionList
		prepare   []string
	)

	// 删除: api中只负责管理shard spec节点的增删，异步守护任务负责干活
	for hbShardId, containerId := range hbShardIdAndContainerId {
		if _, ok := shardIdAndManualContainerId[hbShardId]; !ok {
			resultMAL = append(resultMAL, &moveAction{Service: w.service, ShardId: hbShardId, DropEndpoint: containerId})
		}
	}

	// 新增 or 更新
	for shardId, manualContainerId := range shardIdAndManualContainerId {
		containerId, exist := hbShardIdAndContainerId[shardId]
		if !exist {
			// 不是存活的shard
			if manualContainerId != "" {
				resultMAL = append(resultMAL, &moveAction{Service: w.service, ShardId: shardId, AddEndpoint: manualContainerId})
				continue
			}

			prepare = append(prepare, shardId)
			continue
		}

		// 存活shard
		if manualContainerId != "" {
			if containerId != manualContainerId {
				resultMAL = append(resultMAL, &moveAction{Service: w.service, ShardId: shardId, DropEndpoint: containerId, AddEndpoint: manualContainerId})

				// 存活的shard需要移动，需要从surviveShardIdAndContainerId清理掉，该map中只保存不动的shard
				delete(hbShardIdAndContainerId, shardId)
			}
			continue
		}

		// 无manual分配关系，以自己的container是否存活
		if !surviveContainerIdAndAny.Exist(containerId) {
			prepare = append(prepare, shardId)
		}
	}
	return prepare, resultMAL
}

func (w *maintenanceWorker) parseAssignment(assignment map[string][]string, surviveContainerIdAndAny ArmorMap, surviveShardIdAndContainerId ArmorMap) moveActionList {
	// 新增 or 修改，删除操作通过api直接到达被管理服务，停掉后ShardServer不再有hb上传
	var resultMAL moveActionList
	for newContainerId, shardIds := range assignment {
		for _, shardId := range shardIds {
			curContainerId, ok := surviveShardIdAndContainerId[shardId]
			if !ok {
				// shard不再任何container内部
				resultMAL = append(resultMAL, &moveAction{Service: w.service, ShardId: shardId, AddEndpoint: newContainerId})
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
			if curContainerId == newContainerId {
				w.lg.Error("performAssignment encounter unexpected equal",
					zap.String("shardId", shardId),
					zap.String("containerId", newContainerId),
				)
				continue
			}

			// curContainerId 可能不存在与containerIdAndValue，你给他发drop，可能也无法处理，判断是否是survive的container，不是，允许drop
			var allowDrop bool
			if _, ok := surviveContainerIdAndAny[curContainerId]; !ok {
				allowDrop = true
			}

			resultMAL = append(resultMAL, &moveAction{Service: w.service, ShardId: shardId, DropEndpoint: curContainerId, AddEndpoint: newContainerId, AllowDrop: allowDrop})
		}
	}
	return resultMAL
}

func (w *maintenanceWorker) reallocate(shardIdAndManualContainerId ArmorMap, hbContainerIdAndAny ArmorMap, hbShardIdAndContainerId ArmorMap) moveActionList {
	needAssignShardIds, resultMAL := w.extractNeedAssignShardIds(shardIdAndManualContainerId, hbContainerIdAndAny, hbShardIdAndContainerId)

	// 预先对每个container已经有的shard进行聚合，保证数量均分的情况下，最大限度减少shard的移动
	var cws containerWeightList
	keepContainerIdAndShards := hbShardIdAndContainerId.SwapKV()
	for surviveContainerId := range hbContainerIdAndAny {
		ss, ok := keepContainerIdAndShards[surviveContainerId]
		if ok {
			cws = append(cws, &containerWeight{surviveContainerId, ss})
		} else {
			cws = append(cws, &containerWeight{surviveContainerId, nil})
		}
	}

	total := len(shardIdAndManualContainerId)
	assignment, twice, err := performAssignment(total, needAssignShardIds, cws)
	if err != nil {
		w.lg.Error("failed to performAssignment",
			zap.Error(err),
			zap.Int("total", len(shardIdAndManualContainerId)),
			zap.Strings("needAssignShardIds", needAssignShardIds),
			zap.Reflect("cws", cws),
		)
		return nil
	}
	if len(twice) > 0 {
		r, _, err := performAssignment(total, twice, cws)
		if err != nil {
			w.lg.Error("failed to performAssignment twice",
				zap.Error(err),
				zap.Int("total", len(shardIdAndManualContainerId)),
				zap.Strings("needAssignShardIds", needAssignShardIds),
				zap.Reflect("cws", cws),
			)
			return nil
		}
		for containerId, shardIds := range r {
			if _, ok := assignment[containerId]; !ok {
				assignment[containerId] = shardIds
			} else {
				assignment[containerId] = append(assignment[containerId], shardIds...)
			}
		}
	}
	if len(assignment) == 0 {
		return nil
	}
	w.lg.Debug("perform assignment start",
		zap.String("service", w.service),
		zap.Int("total", total),
		zap.Strings("needAssignShardIds", needAssignShardIds),
		zap.Reflect("cws", cws),
		zap.Reflect("assignment", assignment),
	)

	tmpMAL := w.parseAssignment(assignment, hbContainerIdAndAny, hbShardIdAndContainerId)
	resultMAL = append(resultMAL, tmpMAL...)
	if len(resultMAL) == 0 {
		return nil
	}
	w.lg.Info("perform assignment complete",
		zap.String("service", w.service),
		zap.Reflect("resultMAL", resultMAL),
	)
	return resultMAL
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
