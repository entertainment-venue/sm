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

	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())

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
				opts...,
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
				opts...,
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
	groups := make(map[string]*balancerGroup)

	// 获取当前所有shard配置
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
		w.lg.Info("service not init, because no shard registered",
			zap.String("service", w.service),
			zap.String("node", shardKey),
		)
		return nil
	}
	// 支持手动指定container
	shardIdAndGroup := make(ArmorMap)
	for id, value := range etcdShardIdAndAny {
		var ss apputil.ShardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}

		// 按照group聚合
		bg := groups[ss.Group]
		if bg == nil {
			groups[ss.Group] = newBalanceGroup()
		}
		groups[ss.Group].fixShardIdAndManualContainerId[id] = ss.ManualContainerId

		// 建立index
		shardIdAndGroup[id] = ss.Group
	}

	// 获取当前存活shard，存活shard的container分配关系如果命中可以不生产moveAction
	etcdHbShardIdAndValue, err := w.parent.Client.GetKVs(ctx, nodeAppShardHb(w.service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	for id, value := range etcdHbShardIdAndValue {
		var data apputil.ShardHbData
		if err := json.Unmarshal([]byte(value), &data); err != nil {
			return errors.Wrap(err, "")
		}

		group, ok := shardIdAndGroup[id]
		if !ok {
			for _, bg := range groups {
				bg.hbShardIdAndContainerId[id] = data.ContainerId
				// shard的配置删除，代表service想要移除这个shard，为了方便下面的算法，在第一个group中都保留这个shard，group不影响删除操作
				break
			}
			continue
		}
		// shard配置中存在group
		groups[group].hbShardIdAndContainerId[id] = data.ContainerId
	}

	// 现有存活containers
	var etcdHbContainerIdAndAny ArmorMap
	etcdHbContainerIdAndAny, err = w.parent.Client.GetKVs(ctx, nodeAppHbContainer(w.service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	for group, bg := range groups {
		hbContainerIds := etcdHbContainerIdAndAny.KeyList()
		// 没有存活的container，不需要做shard移动
		if len(hbContainerIds) == 0 {
			w.lg.Warn(
				"no survive container",
				zap.String("group", group),
				zap.String("service", w.service),
			)
			continue
		}

		fixShardIds := bg.fixShardIdAndManualContainerId.KeyList()
		hbShardIds := bg.hbShardIdAndContainerId.KeyList()
		// 没有存活分片，且没有分片待分配
		if len(fixShardIds) == 0 && len(hbShardIds) == 0 {
			w.lg.Warn(
				"no survive shard",
				zap.String("group", group),
				zap.String("service", w.service),
			)
			continue
		}

		containerChanged := w.changed(hbContainerIds, bg.hbShardIdAndContainerId.ValueList())
		shardChanged := w.changed(fixShardIds, hbShardIds)
		if !containerChanged && !shardChanged {
			continue
		}
		// 需要保证在变更的情况下是有container可以接受分配的
		if len(etcdHbContainerIdAndAny) == 0 {
			continue
		}
		var typ eventType
		if containerChanged {
			typ = tContainerChanged
		} else if shardChanged {
			typ = tShardChanged
		}

		r := w.reallocate(bg.fixShardIdAndManualContainerId, etcdHbContainerIdAndAny, bg.hbShardIdAndContainerId)
		if len(r) > 0 {
			ev := mvEvent{
				Service:     w.service,
				Type:        typ,
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

			continue
		}
		// 当survive的container为nil的时候，不能形成有效的分配，直接返回即可
		w.lg.Warn("can not reallocate",
			zap.String("service", w.service),
			zap.Bool("container-changed", containerChanged),
			zap.Bool("shard-changed", shardChanged),
			zap.String("group", group),
			zap.Reflect("shardIdAndManualContainerId", bg.fixShardIdAndManualContainerId),
			zap.Strings("etcdHbContainerIds", etcdHbContainerIdAndAny.KeyList()),
			zap.Reflect("hbShardIdAndContainerId", bg.hbShardIdAndContainerId),
		)
	}
	return nil
}

func (w *maintenanceWorker) changed(a []string, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	return !reflect.DeepEqual(a, b)
}

func (w *maintenanceWorker) reallocate(fixShardIdAndManualContainerId ArmorMap, hbContainerIdAndAny ArmorMap, hbShardIdAndContainerId ArmorMap) moveActionList {
	if len(hbContainerIdAndAny) == 0 {
		w.lg.Info(
			"empty container, can not trigger rebalance",
			zap.String("service", w.service),
		)
		return nil
	}

	// 保证shard在hb中上报的container和存活container一致
	containerIdAndHbShardIds := hbShardIdAndContainerId.SwapKV()
	for containerId := range containerIdAndHbShardIds {
		_, ok := hbContainerIdAndAny[containerId]
		if !ok {
			w.lg.Error(
				"container in shard heartbeat do not exist in container heartbeat",
				zap.String("containerId", containerId),
			)
			return nil
		}
	}

	var (
		mals []*moveAction

		// 在最后做shard分配的时候合并到大集合中
		adding []string

		br = &balancer{
			bcs: make(map[string]*balancerContainer),
		}
	)

	// 提取需要被移除的shard
	for hbShardId, containerId := range hbShardIdAndContainerId {
		_, ok := fixShardIdAndManualContainerId[hbShardId]
		if !ok {
			mals = append(
				mals,
				&moveAction{
					Service:      w.service,
					ShardId:      hbShardId,
					DropEndpoint: containerId,
				},
			)
		}
	}
	if len(fixShardIdAndManualContainerId) == 0 {
		w.lg.Info(
			"remove all shard",
			zap.String("service", w.service),
			zap.Reflect("mals", mals),
		)
		return mals
	}

	// 构建container和shard的关系
	for fixShardId, manualContainerId := range fixShardIdAndManualContainerId {
		// 不在container上，可能是新增，确定需要被分配
		currentContainerId, ok := hbShardIdAndContainerId[fixShardId]
		if !ok {
			if manualContainerId != "" {
				mals = append(
					mals,
					&moveAction{
						Service:     w.service,
						ShardId:     fixShardId,
						AddEndpoint: manualContainerId,
					},
				)

				// 确定的指令，要对当前的csm有影响
				br.put(manualContainerId, fixShardId, true)
			} else {
				adding = append(adding, fixShardId)
			}
			continue
		}

		// 不在要求的container上
		if manualContainerId != "" {
			if currentContainerId != manualContainerId {
				mals = append(
					mals,
					&moveAction{
						Service:      w.service,
						ShardId:      fixShardId,
						DropEndpoint: currentContainerId,
						AddEndpoint:  manualContainerId,
					},
				)

				// 确定的指令，要对当前的csm有影响
				br.put(manualContainerId, fixShardId, true)
			} else {
				// 命中manual是不能被移动的
				br.put(currentContainerId, fixShardId, true)
			}
			continue
		}

		br.put(currentContainerId, fixShardId, false)
	}

	// 处理新增container
	for hbContainerId := range hbContainerIdAndAny {
		_, ok := containerIdAndHbShardIds[hbContainerId]
		if !ok {
			br.addContainer(hbContainerId)
		}
	}

	shardLen := len(fixShardIdAndManualContainerId)
	containerLen := len(hbContainerIdAndAny)

	// 每个container最少包含多少shard
	base := shardLen / containerLen
	delta := shardLen % containerLen
	var maxHold int
	if delta > 0 {
		maxHold = base + 1
	} else {
		maxHold = base
	}

	dropFroms := make(map[string]string)
	getDrops := func(bc *balancerContainer) {
		dropCnt := len(bc.shards) - maxHold
		if dropCnt <= 0 {
			return
		}

		for _, bs := range bc.shards {
			// 不能变动的shard
			if bs.isManual {
				continue
			}
			dropFroms[bs.id] = bc.id
			delete(bc.shards, bs.id)
			dropCnt--
			if dropCnt == 0 {
				break
			}
		}
	}
	br.forEach(getDrops)

	// 可以移动的shard，补充到待分配中
	for drop := range dropFroms {
		adding = append(adding, drop)
	}
	if len(adding) > 0 {
		add := func(bc *balancerContainer) {
			addCnt := maxHold - len(bc.shards)
			if addCnt <= 0 {
				return
			}

			idx := 0
			for {
				if idx == addCnt || idx == len(adding) {
					break
				}

				shardId := adding[idx]
				from, ok := dropFroms[shardId]
				if ok {
					mals = append(mals, &moveAction{Service: w.service, ShardId: adding[idx], DropEndpoint: from, AddEndpoint: bc.id})
				} else {
					mals = append(mals, &moveAction{Service: w.service, ShardId: adding[idx], AddEndpoint: bc.id})
				}
				idx++
			}
			adding = adding[idx:]
		}
		br.forEach(add)
	}

	w.lg.Info(
		"rebalance",
		zap.String("service", w.service),
		zap.Reflect("resultMAL", mals),
		zap.Any("fixShardIdAndManualContainerId", fixShardIdAndManualContainerId),
		zap.Strings("hbContainerIdAndAny", hbContainerIdAndAny.KeyList()),
		zap.Any("hbShardIdAndContainerId", hbShardIdAndContainerId),
	)
	return mals
}

func (w *maintenanceWorker) shardLoadChecker(_ context.Context, ev *clientv3.Event) error {
	// 只关注hb节点的load变化
	if !ev.IsModify() {
		return nil
	}
	// TODO 判断本次load时间是否需要出发shard move
	return nil

	start := time.Now()
	qev := mvEvent{
		Service:     w.service,
		Type:        tShardLoadChanged,
		EnqueueTime: start.Unix(),
		Value:       string(ev.Kv.Value),
	}
	item := Item{Priority: start.Unix(), Value: qev.String()}
	w.parent.eq.push(&item, true)
	return nil
}

func (w *maintenanceWorker) containerLoadChecker(_ context.Context, ev *clientv3.Event) error {
	if !ev.IsModify() {
		return nil
	}
	// TODO 判断本次load时间是否需要出发shard move
	return nil

	start := time.Now()
	qev := mvEvent{
		Service:     w.service,
		Type:        tContainerLoadChanged,
		EnqueueTime: start.Unix(),
		Value:       string(ev.Kv.Value),
	}
	item := Item{Priority: start.Unix(), Value: qev.String()}
	w.parent.eq.push(&item, true)
	return nil
}
