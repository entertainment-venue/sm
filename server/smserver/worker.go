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
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	"go.uber.org/zap"
)

type workerEventType int

const (
	tShardChanged workerEventType = iota + 1
	tShardLoadChanged
	tContainerChanged
	tContainerLoadChanged
	tContainerInit

	workerTrigger = "workerTrigger"

	defaultMaxShardCount = math.MaxInt
)

type workerTriggerEvent struct {
	// Service 预留
	Service string `json:"service"`

	// Type 预留，用于在process中区分事件类型进行处理
	Type workerEventType `json:"type"`

	// EnqueueTime 预留，防止需要做延时
	EnqueueTime int64 `json:"enqueueTime"`

	// Value 存储moveActionList
	Value []byte `json:"value"`
}

// Worker 管理某个sm app的shard
type Worker struct {
	parent  *smContainer
	lg      *zap.Logger
	stopper *apputil.GoroutineStopper

	// service 从属于leader或者sm smShard，service和container不一定一样
	service string

	// spec 需要通过配置影响balance算法
	spec *smAppSpec

	// mpr 存储当前存活的container和shard信息，代理etcd访问
	mpr *mapper

	// trigger 负责分片移动任务的任务提交和处理
	trigger *evtrigger.Trigger
	// operator 对接接入方，通过http请求下发shard move指令
	operator *operator
}

func newWorker(lg *zap.Logger, container *smContainer, service string) (*Worker, error) {
	// worker需要service的配置信息，作为balance的因素
	appSpecNode := nodeAppSpec(service)
	resp, err := container.Client.GetKV(context.TODO(), appSpecNode, nil)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		err := errors.Errorf("service not config %s", appSpecNode)
		return nil, errors.Wrap(err, "")
	}
	appSpec := smAppSpec{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &appSpec); err != nil {
		return nil, errors.Wrap(err, "")
	}
	if appSpec.MaxShardCount <= 0 {
		appSpec.MaxShardCount = defaultMaxShardCount
	}

	w := &Worker{
		lg:      lg,
		parent:  container,
		service: service,
		stopper: &apputil.GoroutineStopper{},
		spec:    &appSpec,
	}

	// 封装事件异步处理
	trigger, _ := evtrigger.NewTrigger(
		evtrigger.WithLogger(lg),
		evtrigger.WithWorkerSize(1),
	)
	_ = trigger.Register(workerTrigger, w.processEvent)
	w.trigger = trigger
	w.operator = newOperator(lg, container, service)

	// TODO 参数传递的有些冗余，需要重新梳理
	w.mpr, err = newMapper(lg, container, &appSpec)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	w.stopper.Wrap(
		func(ctx context.Context) {
			apputil.TickerLoop(
				ctx,
				w.lg,
				defaultLoopInterval,
				fmt.Sprintf("[lw] service %s ShardAllocateLoop exit", w.service),
				func(ctx context.Context) error {
					return w.allocateChecker(ctx)
				},
			)
		},
	)

	w.lg.Info("Worker started", zap.String("service", w.service))
	return w, nil
}

func (w *Worker) SetMaxShardCount(maxShardCount int) {
	if maxShardCount > 0 {
		w.spec.MaxShardCount = maxShardCount
	}
}

func (w *Worker) SetMaxRecoveryTime(maxRecoveryTime int) {
	if maxRecoveryTime > 0 && time.Duration(maxRecoveryTime)*time.Second <= maxRecoveryWaitTime {
		w.mpr.maxRecoveryTime = time.Duration(maxRecoveryTime) * time.Second
	}
}

func (w *Worker) Close() {
	w.mpr.Close()
	w.stopper.Close()
	w.lg.Info("Worker stopped", zap.String("service", w.service))
}

// 1 smContainer 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 smShard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func (w *Worker) allocateChecker(ctx context.Context) error {
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
	etcdHbShardIdAndValue := w.mpr.AliveShards()
	for shardId, value := range etcdHbShardIdAndValue {
		group, ok := shardIdAndGroup[shardId]
		if !ok {
			for _, bg := range groups {
				bg.hbShardIdAndContainerId[shardId] = value.curContainerId
				// shard的配置删除，代表service想要移除这个shard，为了方便下面的算法，在第一个group中都保留这个shard，group不影响删除操作
				break
			}
			continue
		}
		// shard配置中存在group
		groups[group].hbShardIdAndContainerId[shardId] = value.curContainerId
	}

	// 现有存活containers
	etcdHbContainerIdAndAny := w.mpr.AliveContainers()

	// 增加阈值限制，防止单进程过载导致雪崩
	maxHold := w.maxHold(len(etcdHbContainerIdAndAny), len(etcdShardIdAndAny))
	if maxHold > w.spec.MaxShardCount {
		err := errors.New("MaxShardCount exceeded")
		w.lg.Error(
			err.Error(),
			zap.String("service", w.service),
			zap.Int("maxHold", maxHold),
			zap.Int("containerCnt", len(etcdHbContainerIdAndAny)),
			zap.Int("shardCnt", len(etcdShardIdAndAny)),
			zap.Error(err),
		)
		return err
	}

	for group, bg := range groups {
		hbContainerIds := etcdHbContainerIdAndAny.KeyList()
		// 没有存活的container，不需要做shard移动
		if len(hbContainerIds) == 0 {
			w.lg.Warn(
				"no survive container",
				zap.String("service", w.service),
			)
			break
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
			// 需要探测是否有某个container过载，即超过应该容纳的shard数量
			var exist bool
			maxHold := w.maxHold(len(hbContainerIds), len(fixShardIds))
			kv := bg.hbShardIdAndContainerId.SwapKV()
			for _, shardIds := range kv {
				if len(shardIds) > maxHold {
					exist = true
					break
				}
			}
			if !exist {
				continue
			}
		}

		w.lg.Info(
			"changed",
			zap.String("group", group),
			zap.String("service", w.service),
			zap.Bool("containerChanged", containerChanged),
			zap.Bool("shardChanged", shardChanged),
		)

		// 需要保证在变更的情况下是有container可以接受分配的
		if len(etcdHbContainerIdAndAny) == 0 {
			continue
		}
		var typ workerEventType
		if containerChanged {
			typ = tContainerChanged
		} else {
			typ = tShardChanged
		}

		r := w.reallocate(bg.fixShardIdAndManualContainerId, etcdHbContainerIdAndAny, bg.hbShardIdAndContainerId)
		if len(r) > 0 {
			ev := workerTriggerEvent{
				Service:     w.service,
				Type:        typ,
				EnqueueTime: time.Now().Unix(),
				Value:       []byte(r.String()),
			}
			_ = w.trigger.Put(&evtrigger.TriggerEvent{Key: workerTrigger, Value: &ev})
			w.lg.Info("event enqueue",
				zap.String("service", w.service),
				zap.Reflect("event", ev),
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

func (w *Worker) changed(a []string, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	return !reflect.DeepEqual(a, b)
}

func (w *Worker) reallocate(fixShardIdAndManualContainerId ArmorMap, hbContainerIdAndAny ArmorMap, hbShardIdAndContainerId ArmorMap) moveActionList {
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
	maxHold := w.maxHold(containerLen, shardLen)

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

func (w *Worker) maxHold(containerCnt, shardCnt int) int {
	if containerCnt == 0 {
		// 不做过滤
		return 0
	}
	base := shardCnt / containerCnt
	delta := shardCnt % containerCnt
	var r int
	if delta > 0 {
		r = base + 1
	} else {
		r = base
	}
	return r
}

func (w *Worker) processEvent(key string, value interface{}) error {
	event := value.(*workerTriggerEvent)
	w.lg.Info(
		"event received",
		zap.String("key", key),
		zap.Reflect("ev", event),
	)
	if err := w.operator.move(context.TODO(), event.Value); err != nil {
		w.lg.Error(
			"move error",
			zap.String("key", key),
			zap.Reflect("ev", event),
			zap.Error(err),
		)
		return errors.Wrap(err, "")
	}
	return nil
}
