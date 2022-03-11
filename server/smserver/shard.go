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
	"sync"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	_ Shard        = new(smShard)
	_ ShardWrapper = new(smShardWrapper)
)

const (
	workerTrigger = "workerTrigger"

	defaultMaxShardCount = math.MaxInt

	// defaultBridgeLeaseTimeout 单位s
	defaultBridgeLeaseTimeout = 10

	// defaultGuardLeaseTimeout 单位s
	defaultGuardLeaseTimeout = 10
)

type workerTriggerEvent struct {
	// Service 预留
	Service string `json:"service"`

	// EnqueueTime 预留，防止需要做延时
	EnqueueTime int64 `json:"enqueueTime"`

	// Value 存储moveActionList
	Value []byte `json:"value"`
}

// smShardWrapper 实现 ShardWrapper，4 unit test
type smShardWrapper struct {
	ss *smShard
}

func (s *smShardWrapper) NewShard(c *smContainer, spec *apputil.ShardSpec) (Shard, error) {
	return newSMShard(c, spec)
}

// sm的任务: 管理governedService的container和shard监控
type shardTask struct {
	GovernedService string `json:"governedService"`
}

func (t *shardTask) String() string {
	b, _ := json.Marshal(t)
	return string(b)
}

func (t *shardTask) Validate() bool {
	return t.GovernedService != ""
}

// smShard 管理某个sm app的shard
type smShard struct {
	container *smContainer
	lg        *zap.Logger
	stopper   *apputil.GoroutineStopper

	// service 从属于leader或者sm smShard，service和container不一定一样
	service string
	// appSpec 需要通过配置影响balance算法
	appSpec *smAppSpec
	// shardSpec 分片的配置信息
	shardSpec *apputil.ShardSpec

	// mpr 存储当前存活的container和shard信息，代理etcd访问
	mpr *mapper

	// trigger 负责分片移动任务的任务提交和处理
	trigger *evtrigger.Trigger
	// operator 对接接入方，通过http请求下发shard move指令
	operator *operator

	mu sync.Mutex
	// balancing 正在rb的情况下，不能开启下一次rb
	balancing bool
}

func newSMShard(container *smContainer, shardSpec *apputil.ShardSpec) (*smShard, error) {
	ss := &smShard{
		container: container,
		shardSpec: shardSpec,
		stopper:   &apputil.GoroutineStopper{},
		lg:        container.lg,
	}

	// 解析任务中需要负责的service
	var st shardTask
	if err := json.Unmarshal([]byte(shardSpec.Task), &st); err != nil {
		return nil, errors.Wrap(err, "")
	}
	ss.service = st.GovernedService

	// worker需要service的配置信息，作为balance的因素
	serviceSpec := container.nodeManager.nodeServiceSpec(ss.service)
	resp, err := container.Client.GetKV(context.TODO(), serviceSpec, nil)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		err := errors.Errorf("service not config %s", serviceSpec)
		return nil, errors.Wrap(err, "")
	}

	appSpec := smAppSpec{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &appSpec); err != nil {
		return nil, errors.Wrap(err, "")
	}
	if appSpec.MaxShardCount <= 0 {
		appSpec.MaxShardCount = defaultMaxShardCount
	}
	ss.appSpec = &appSpec

	// 封装事件异步处理
	trigger, _ := evtrigger.NewTrigger(
		evtrigger.WithLogger(ss.lg),
		evtrigger.WithWorkerSize(1),
	)
	_ = trigger.Register(workerTrigger, ss.processEvent)
	ss.trigger = trigger
	ss.operator = newOperator(ss.lg, shardSpec.Service)

	// TODO 参数传递的有些冗余，需要重新梳理
	ss.mpr, err = newMapper(container, &appSpec)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	ss.stopper.Wrap(
		func(ctx context.Context) {
			apputil.TickerLoop(
				ctx,
				ss.lg,
				defaultLoopInterval,
				fmt.Sprintf("balanceChecker exit, service %s ", ss.service),
				func(ctx context.Context) error {
					return ss.balanceChecker(ctx)
				},
			)
		},
	)

	ss.lg.Info("smShard started", zap.String("service", ss.service))
	return ss, nil
}

func (ss *smShard) SetMaxShardCount(maxShardCount int) {
	if maxShardCount > 0 {
		ss.appSpec.MaxShardCount = maxShardCount
	}
}

func (ss *smShard) SetMaxRecoveryTime(maxRecoveryTime int) {
	if maxRecoveryTime > 0 && time.Duration(maxRecoveryTime)*time.Second <= maxRecoveryWaitTime {
		ss.mpr.maxRecoveryTime = time.Duration(maxRecoveryTime) * time.Second
	}
}

func (ss *smShard) Spec() *apputil.ShardSpec {
	return ss.shardSpec
}

func (ss *smShard) Close() error {
	ss.mpr.Close()

	ss.trigger.Close()
	ss.lg.Info(
		"trigger closing",
		zap.String("service", ss.service),
	)

	ss.stopper.Close()
	ss.lg.Info(
		"smShard closing",
		zap.String("service", ss.service),
	)
	return nil
}

// 1 smContainer 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 smShard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func (ss *smShard) balanceChecker(ctx context.Context) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// rb中，没必要再次rb
	// 当前进行的rb要根据container存活现状（方法调用时刻的一个切面）计算策略
	if ss.balancing {
		ss.lg.Info("balancing", zap.String("service", ss.service))
		return nil
	}
	ss.balancing = true
	defer func() {
		ss.balancing = false
	}()

	// 获取guard lease
	if err := ss.validateGuardLease(); err != nil {
		ss.lg.Error(
			"validateGuardLease error",
			zap.String("service", ss.service),
			zap.Error(err),
		)
		return err
	}

	// 现有存活containers
	etcdHbContainerIdAndAny := ss.mpr.AliveContainers()
	// 没有存活的container，不需要做shard移动
	if len(etcdHbContainerIdAndAny) == 0 {
		ss.lg.Info(
			"no alive container",
			zap.String("service", ss.service),
		)
		return nil
	}

	// 获取当前所有shard配置
	var (
		etcdShardIdAndAny ArmorMap
		err               error

		// 针对特定service，区分group做rb，允许同一service可以划分多个小的业务场景
		groups = make(map[string]*balancerGroup)
	)
	shardKey := ss.container.nodeManager.nodeServiceShard(ss.service, "")
	etcdShardIdAndAny, err = ss.container.Client.GetKVs(ctx, shardKey)
	if err != nil {
		return errors.Wrap(err, "")
	}
	// 支持手动指定container
	shardIdAndGroup := make(ArmorMap)
	// 提供给 moveAction，做内容下发，防止sdk再次获取，sdk不会有sm空间的访问权限
	shardIdAndShardSpec := make(map[string]*apputil.ShardSpec)
	for id, value := range etcdShardIdAndAny {
		var ss apputil.ShardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}
		shardIdAndShardSpec[id] = &ss

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
	etcdHbShardIdAndValue := ss.mpr.AliveShards()
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

	// allShardMoves 收集所有的moveAction，用于在checker最后做guard lease的机制
	var allShardMoves moveActionList

	// shard被清除的场景，从rebalance方法中提前到这里，应对完全不配置shard，且sdk本地存活的场景
	// 提取需要被移除的shard
	var deleting moveActionList
	for hbShardId, value := range etcdHbShardIdAndValue {
		if _, ok := etcdShardIdAndAny[hbShardId]; !ok {
			deleting = append(
				deleting,
				&moveAction{
					Service:      ss.service,
					ShardId:      hbShardId,
					DropEndpoint: value.curContainerId,
				},
			)
			delete(etcdHbShardIdAndValue, hbShardId)
		}
	}
	if len(deleting) > 0 {
		allShardMoves = append(allShardMoves, deleting...)
		ss.lg.Info(
			"shard deleting",
			zap.String("service", ss.service),
			zap.Reflect("deleting", deleting),
		)
	} else {
		// shard被清理，但是发现心跳带上来的还有未清理掉的shard，通过rb drop掉
		if len(etcdShardIdAndAny) == 0 {
			ss.lg.Info(
				"no shard configured",
				zap.String("service", ss.service),
			)
			return nil
		}
	}

	// 增加阈值限制，防止单进程过载导致雪崩
	maxHold := ss.maxHold(len(etcdHbContainerIdAndAny), len(etcdShardIdAndAny))
	if maxHold > ss.appSpec.MaxShardCount {
		err := errors.New("MaxShardCount exceeded")
		ss.lg.Error(
			err.Error(),
			zap.String("service", ss.service),
			zap.Int("maxHold", maxHold),
			zap.Int("containerCnt", len(etcdHbContainerIdAndAny)),
			zap.Int("shardCnt", len(etcdShardIdAndAny)),
			zap.Error(err),
		)
		return err
	}

	// 现存shard的分配
	for group, bg := range groups {
		hbContainerIds := etcdHbContainerIdAndAny.KeyList()
		fixShardIds := bg.fixShardIdAndManualContainerId.KeyList()
		hbShardIds := bg.hbShardIdAndContainerId.KeyList()
		// 没有存活分片，且没有分片待分配
		if len(fixShardIds) == 0 && len(hbShardIds) == 0 {
			ss.lg.Warn(
				"no survive shard",
				zap.String("group", group),
				zap.String("service", ss.service),
			)
			continue
		}

		containerChanged := ss.changed(hbContainerIds, bg.hbShardIdAndContainerId.ValueList())
		shardChanged := ss.changed(fixShardIds, hbShardIds)
		if !containerChanged && !shardChanged {
			// 需要探测是否有某个container过载，即超过应该容纳的shard数量
			var exist bool
			maxHold := ss.maxHold(len(hbContainerIds), len(fixShardIds))
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

		ss.lg.Info(
			"changed",
			zap.String("group", group),
			zap.String("service", ss.service),
			zap.Bool("containerChanged", containerChanged),
			zap.Bool("shardChanged", shardChanged),
			zap.Reflect("hbContainerIds", hbContainerIds),
			zap.Reflect("bg.hbShardIdAndContainerId.ValueList())", bg.hbShardIdAndContainerId.ValueList()),
			zap.Reflect("fixShardIds", fixShardIds),
			zap.Reflect("hbShardIds", hbShardIds),
		)

		// 需要保证在变更的情况下是有container可以接受分配的
		if len(etcdHbContainerIdAndAny) == 0 {
			continue
		}

		shardMoves := ss.extractShardMoves(bg.fixShardIdAndManualContainerId, etcdHbContainerIdAndAny, bg.hbShardIdAndContainerId, shardIdAndShardSpec)
		if len(shardMoves) > 0 {
			allShardMoves = append(allShardMoves, shardMoves...)
			continue
		}
		// 当survive的container为nil的时候，不能形成有效的分配，直接返回即可
		ss.lg.Warn(
			"can not extractShardMoves",
			zap.String("service", ss.service),
			zap.Bool("container-changed", containerChanged),
			zap.Bool("shard-changed", shardChanged),
			zap.String("group", group),
			zap.Reflect("shardIdAndManualContainerId", bg.fixShardIdAndManualContainerId),
			zap.Strings("etcdHbContainerIds", etcdHbContainerIdAndAny.KeyList()),
			zap.Reflect("hbShardIdAndContainerId", bg.hbShardIdAndContainerId),
		)
	}

	// guard lease 实现
	if len(allShardMoves) > 0 {
		ss.lg.Info(
			"service start rb",
			zap.String("service", ss.service),
		)
		if err := ss.rb(allShardMoves); err != nil {
			return errors.Wrap(err, "")
		}
	} else {
		ss.lg.Info(
			"service safe, no shard moves",
			zap.String("service", ss.service),
		)
	}
	return nil
}

func (ss *smShard) rb(shardMoves moveActionList) error {
	// 1 先梳理出来drop的shard
	assignment := apputil.Assignment{}
	for _, action := range shardMoves {
		if action.DropEndpoint != "" {
			assignment.Drops = append(assignment.Drops, action.ShardId)
		}
	}
	// 2 获取新的bridge lease
	bridgeGrantLeaseResp, err := ss.container.Client.GetClient().Grant(context.TODO(), defaultBridgeLeaseTimeout)
	if err != nil {
		return errors.Wrap(err, "Grant error")
	}
	// 3 写入bridge lease节点
	bridgeLease := apputil.Lease{
		ID:         bridgeGrantLeaseResp.ID,
		Assignment: &assignment,
	}
	bridgePfx := ss.container.nodeManager.nodeServiceBridge(ss.service)
	if err := ss.container.Client.CreateAndGet(context.TODO(), []string{bridgePfx}, []string{bridgeLease.String()}, bridgeGrantLeaseResp.ID); err != nil {
		return errors.Wrap(err, "CreateAndGet error")
	}
	// 4 等待bridge timeout
	bridgeLeaseTimeToLiveResponse, err := ss.container.Client.GetClient().TimeToLive(context.TODO(), bridgeLease.ID)
	if err != nil {
		return errors.Wrap(err, "TimeToLive error")
	}
	if bridgeLeaseTimeToLiveResponse.TTL == -1 {
		ss.lg.Info(
			"bridge lease already expired",
			zap.String("bridgePfx", bridgePfx),
			zap.Int64("ttl", bridgeLeaseTimeToLiveResponse.TTL),
			zap.Reflect("lease", bridgeLease),
		)
	} else {
		ss.lg.Info(
			"start waiting bridge lease expired",
			zap.String("bridgePfx", bridgePfx),
			zap.Int64("ttl", bridgeLeaseTimeToLiveResponse.TTL),
			zap.Reflect("lease", bridgeLease),
		)
		time.Sleep(time.Duration(bridgeLeaseTimeToLiveResponse.TTL+1) * time.Second)
		ss.lg.Info(
			"bridge lease expired",
			zap.String("bridgePfx", bridgePfx),
			zap.Reflect("lease", bridgeLease),
		)
	}
	if _, err := ss.container.Client.GetClient().Revoke(context.TODO(), bridgeLease.ID); err != nil {
		return errors.Wrap(err, "Revoke error")
	}

	// 5 grant guard lease
	guardLeaseResp, err := ss.container.Client.GetClient().Grant(context.TODO(), defaultGuardLeaseTimeout)
	if err != nil {
		return errors.Wrap(err, "Grant error")
	}
	// 6 设置guard lease
	guardPfx := ss.container.nodeManager.nodeServiceGuard(ss.service)
	guardLease := apputil.Lease{ID: guardLeaseResp.ID}
	if _, err := ss.container.Client.Put(context.TODO(), guardPfx, guardLease.String()); err != nil {
		return errors.Wrap(err, "Put error")
	}
	// 7 等待guard timeout
	guardLeaseTimeToLiveResponse, err := ss.container.Client.GetClient().TimeToLive(context.TODO(), guardLeaseResp.ID)
	if err != nil {
		return errors.Wrap(err, "TimeToLive error")
	}
	ss.lg.Info(
		"waiting guard lease expired",
		zap.String("guardPfx", guardPfx),
		zap.Int64("ttl", guardLeaseTimeToLiveResponse.TTL),
		zap.Reflect("lease", guardLease),
	)
	time.Sleep(time.Duration(guardLeaseTimeToLiveResponse.TTL+1) * time.Second)
	ss.lg.Info(
		"guard lease expired",
		zap.String("bridgePfx", bridgePfx),
		zap.Reflect("lease", guardLease),
	)
	// 8 下发add请求
	var addMA moveActionList
	shards := ss.mpr.AliveShards()
	for _, action := range shardMoves {
		if action.AddEndpoint == "" {
			continue
		}

		t, ok := shards[action.ShardId]
		if !ok {
			// 不是存活shard，可以移动
			action.DropEndpoint = ""
			action.Spec.LeaseID = guardLease.ID
			addMA = append(addMA, action)
			continue
		}

		// shard存活，打印日志报告异常
		if t.leaseID != guardLease.ID {
			ss.lg.Error(
				"alive shard invalid lease",
				zap.String("shardId", action.ShardId),
				zap.Reflect("t", t),
				zap.Int64("leaseID", int64(t.leaseID)),
				zap.Int64("guardLease", int64(guardLease.ID)),
			)
		}
	}
	if len(addMA) > 0 {
		ev := workerTriggerEvent{
			Service:     ss.service,
			EnqueueTime: time.Now().Unix(),
			Value:       []byte(addMA.String()),
		}
		_ = ss.trigger.Put(&evtrigger.TriggerEvent{Key: workerTrigger, Value: &ev})
	}

	// 9 要等shardkeeper内部的分片下发，且方式立即开启一次rb
	time.Sleep(5 * time.Second)

	return nil
}

func (ss *smShard) validateGuardLease() error {
	// 获取guard lease
	guardPfx := ss.container.nodeManager.nodeServiceGuard(ss.service)
	resp, err := ss.container.Client.GetKV(context.TODO(), guardPfx, []clientv3.OpOption{clientv3.WithPrefix()})
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		err := errors.Errorf("guard not found %s", guardPfx)
		return errors.Wrap(err, "")
	}
	var lease apputil.Lease
	if err := json.Unmarshal(resp.Kvs[0].Value, &lease); err != nil {
		return errors.Wrap(err, "")
	}
	if lease.ID == clientv3.NoLease {
		ss.lg.Info(
			"guard NoLease",
			zap.String("guardPfx", guardPfx),
		)
	} else {
		ss.lg.Info(
			"current guard lease",
			zap.String("guardPfx", guardPfx),
			zap.Int64("leaseID", int64(lease.ID)),
		)
	}
	return nil
}

func (ss *smShard) changed(a []string, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	return !reflect.DeepEqual(a, b)
}

// 只负责shard移动的场景，删除在balanceChecker中处理
func (ss *smShard) extractShardMoves(
	fixShardIdAndManualContainerId ArmorMap,
	hbContainerIdAndAny ArmorMap,
	hbShardIdAndContainerId ArmorMap,
	shardIdAndShardSpec map[string]*apputil.ShardSpec) moveActionList {
	// 保证shard在hb中上报的container和存活container一致
	containerIdAndHbShardIds := hbShardIdAndContainerId.SwapKV()
	for containerId := range containerIdAndHbShardIds {
		_, ok := hbContainerIdAndAny[containerId]
		if !ok {
			ss.lg.Error(
				"container in shard heartbeat do not exist in container heartbeat",
				zap.String("containerId", containerId),
			)
			return nil
		}
	}

	var (
		mals moveActionList

		// 在最后做shard分配的时候合并到大集合中
		adding []string

		br = &balancer{
			bcs: make(map[string]*balancerContainer),
		}
	)

	// 构建container和shard的关系
	for fixShardId, manualContainerId := range fixShardIdAndManualContainerId {
		// 不在container上，可能是新增，确定需要被分配
		currentContainerId, ok := hbShardIdAndContainerId[fixShardId]
		if !ok {
			if manualContainerId != "" {
				spec := shardIdAndShardSpec[fixShardId]
				mals = append(
					mals,
					&moveAction{
						Service:     ss.service,
						ShardId:     fixShardId,
						AddEndpoint: manualContainerId,
						Spec:        spec,
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
				spec := shardIdAndShardSpec[fixShardId]
				mals = append(
					mals,
					&moveAction{
						Service:      ss.service,
						ShardId:      fixShardId,
						DropEndpoint: currentContainerId,
						AddEndpoint:  manualContainerId,
						Spec:         spec,
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
	maxHold := ss.maxHold(containerLen, shardLen)

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
				spec := shardIdAndShardSpec[shardId]
				from, ok := dropFroms[shardId]
				if ok {
					mals = append(
						mals,
						&moveAction{
							Service:      ss.service,
							ShardId:      adding[idx],
							DropEndpoint: from,
							AddEndpoint:  bc.id,
							Spec:         spec,
						},
					)
				} else {
					mals = append(
						mals,
						&moveAction{
							Service:     ss.service,
							ShardId:     adding[idx],
							AddEndpoint: bc.id,
							Spec:        spec,
						},
					)
				}
				idx++
			}
			adding = adding[idx:]
		}
		br.forEach(add)
	}

	ss.lg.Info(
		"rb result",
		zap.String("service", ss.service),
		zap.Reflect("resultMAL", mals),
		zap.Any("fixShardIdAndManualContainerId", fixShardIdAndManualContainerId),
		zap.Strings("hbContainerIdAndAny", hbContainerIdAndAny.KeyList()),
		zap.Any("hbShardIdAndContainerId", hbShardIdAndContainerId),
	)
	return mals
}

func (ss *smShard) maxHold(containerCnt, shardCnt int) int {
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

func (ss *smShard) processEvent(key string, value interface{}) error {
	event := value.(*workerTriggerEvent)

	var mal moveActionList
	if err := json.Unmarshal(event.Value, &mal); err != nil {
		ss.lg.Error(
			"Unmarshal error",
			zap.ByteString("value", event.Value),
			zap.Error(err),
		)
		// return ASAP unmarshal失败重试没意义，需要人工接入进行数据修正
		return errors.Wrap(err, "")
	}
	ss.lg.Info(
		"event received",
		zap.String("key", key),
		zap.Reflect("mal", mal),
	)
	if len(mal) == 0 {
		ss.lg.Warn(
			"empty move actions",
			zap.String("key", key),
		)
		return nil
	}

	if err := ss.operator.move(mal); err != nil {
		ss.lg.Error(
			"move error",
			zap.String("key", key),
			zap.Reflect("ev", event),
			zap.Error(err),
		)
		return errors.Wrap(err, "")
	}
	return nil
}
