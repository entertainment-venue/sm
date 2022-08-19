package smserver

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// 通过key的区分，在UpdateState中区分情况处理，降低代码冗余
	containerTrigger = "containerTrigger"

	// triggerWorkerSize 必须是1，事件顺序执行
	triggerWorkerSize = 1

	// defaultMaxRecoveryTime 不设定，需要提供默认阈值，理论上应该能应对大多应用的重启时间
	defaultMaxRecoveryTime = 10 * time.Second
	// maxRecoveryWaitTime 给个上限，防止异常情况导致等待时间过长问题排查难
	maxRecoveryWaitTime = 30 * time.Second
)

// mapper leader或者follower都需要构建当前分片应用的映射关系
// https://github.com/entertainment-venue/sm/wiki/%E9%97%AE%E9%A2%98:-%E5%88%86%E7%89%87%E5%BA%94%E7%94%A8%E9%87%8D%E5%90%AF%E5%AF%BC%E8%87%B4%E5%A4%A7%E9%87%8F%E5%88%86%E7%89%87%E7%A7%BB%E5%8A%A8%E8%AF%B7%E6%B1%82
// 1. 快速提取存活container列表，减少对etcd的周期访问
// 2. 参考chubby，master在rb这块，允许分片节点上报自己负责的分片，类似于chunkserver
// 3. 代理mw对etcd的访问，分摊部分mw的逻辑
type mapper struct {
	container *smContainer
	lg        *zap.Logger
	shard     *smShard

	// appSpec 配置中有container单节点恢复阈值，影响当前service事件处理的方式
	appSpec *smAppSpec
	// maxRecoveryTime 应对集群滚动重启场景，等待出现del事件出现，等待一段时间后，尝试恢复动作，减少
	maxRecoveryTime time.Duration

	mu sync.Mutex
	// containerState 存活container
	containerState *mapperState
	// shardState 存活shard
	shardState *mapperState

	// trigger 事件存储在内存中的队列里，逐一执行，尽量不卡在etcd，因为事件丢失是可恢复的
	trigger evtrigger.Trigger

	// stopper 管理watch goroutine
	stopper *apputil.GoroutineStopper
}

func newMapper(container *smContainer, appSpec *smAppSpec, shard *smShard) (*mapper, error) {
	mpr := mapper{
		container: container,
		lg:        container.lg,
		appSpec:   appSpec,
		stopper:   &apputil.GoroutineStopper{},
		shard:     shard,

		containerState: newMapperState(),
		shardState:     newMapperState(),
	}

	trigger, _ := evtrigger.NewTrigger(
		evtrigger.WithLogger(mpr.lg),
		evtrigger.WithWorkerSize(triggerWorkerSize),
	)
	mpr.trigger = trigger
	_ = mpr.trigger.Register(containerTrigger, mpr.UpdateState)

	if mpr.appSpec.MaxRecoveryTime <= 0 || time.Duration(mpr.appSpec.MaxRecoveryTime)*time.Second > maxRecoveryWaitTime {
		mpr.maxRecoveryTime = defaultMaxRecoveryTime
	} else {
		mpr.maxRecoveryTime = time.Duration(mpr.appSpec.MaxRecoveryTime) * time.Second
	}

	if err := mpr.initAndWatch(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	mpr.lg.Info(
		"mapper started",
		zap.String("service", mpr.appSpec.Service),
	)

	return &mpr, nil
}

func (mpr *mapper) extractId(key string) string {
	// https://github.com/entertainment-venue/sm/commit/77c6ba8d36196b6fa5a115483083ae9777f70c7d
	// 目录结构引入mutex，导致有变化，id在倒数第二段
	arr := strings.Split(key, "/")
	str := arr[len(arr)-2]
	if str == "" {
		mpr.lg.Panic(
			"key error",
			zap.String("key", key),
		)
	}
	return str
}

func (mpr *mapper) initAndWatch() error {
	pfx := mpr.container.nodeManager.ExternalContainerHbDir(mpr.appSpec.Service)
	getOpts := []clientv3.OpOption{clientv3.WithPrefix()}
	resp, err := mpr.container.Client.Get(context.TODO(), pfx, getOpts...)
	if err != nil {
		return err
	}
	startRev := resp.Header.Revision + 1

	mpr.stopper.Wrap(
		func(ctx context.Context) {
			apputil.WatchLoop(
				ctx,
				mpr.lg,
				mpr.container.Client,
				pfx,
				startRev,
				func(ctx context.Context, ev *clientv3.Event) error {
					// 理论上不会有这种event，containerhb节点是用于记录revision的，让watch有个开始的地方
					if string(ev.Kv.Key) == pfx {
						return nil
					}

					// 事件写入evtrigger，理论上不会漏事件，除非event不合法
					if err := mpr.trigger.Put(&evtrigger.TriggerEvent{Key: containerTrigger, Value: ev}); err != nil {
						return errors.Wrap(err, "")
					}
					return nil
				},
			)
		},
	)
	mpr.lg.Info(
		"watch hb",
		zap.String("pfx", pfx),
		zap.String("service", mpr.appSpec.Service),
	)
	return nil
}

func (mpr *mapper) AliveContainers() ArmorMap {
	mpr.mu.Lock()
	defer mpr.mu.Unlock()

	r := make(ArmorMap)
	collectId := func(id string, tmp *temporary) error {
		r[id] = ""
		return nil
	}
	_ = mpr.containerState.ForEach(collectId)
	return r
}

func (mpr *mapper) AliveShards() map[string]*temporary {
	mpr.mu.Lock()
	defer mpr.mu.Unlock()

	r := make(map[string]*temporary)
	collectId := func(id string, tmp *temporary) error {
		if tmp.leaseID == mpr.shard.guardLeaseID {
			r[id] = tmp
		} else {
			delete(mpr.shardState.alive, id)
		}
		return nil
	}
	_ = mpr.shardState.ForEach(collectId)
	return r
}

func (mpr *mapper) Close() {
	if mpr.stopper != nil {
		mpr.stopper.Close()
	}
	mpr.trigger.Close()
}

func (mpr *mapper) UpdateState(_ string, value interface{}) error {
	event := value.(*clientv3.Event)

	containerId := mpr.extractId(string(event.Kv.Key))
	if event.IsCreate() || event.IsModify() {
		// 需要更新container或者shard的存活事件
		return mpr.Refresh(containerId, event)
	}

	// container故障(短暂的网络、硬件问题等等)，或者重启
	if err := mpr.Wait(containerId); err != nil {
		return err
	}

	// 通过后面的时间检测是否container已经回来，这块有两种方案：
	// 1. 轮询探测
	// 2. 遍历evtrigger中的事件，如果有该container的put事件，直接return，否则删除掉
	// 3. 可能会出现该container最终需要删除，但是存在put事件，轮询的时候直接返回了，导致效率可能不是很高。
	// 这里不做最终是否删除判断，等最后一个删除事件自己删除即可
	var hasCreate bool
	findCreate := func(it interface{}) error {
		triggerEvent := it.(*evtrigger.TriggerEvent)
		ev := triggerEvent.Value.(*clientv3.Event)
		if mpr.extractId(string(ev.Kv.Key)) == containerId && ev.IsCreate() {
			hasCreate = true
		}
		return nil
	}
	_ = mpr.trigger.ForEach(findCreate)
	if hasCreate {
		mpr.lg.Info(
			"container recovery",
			zap.String("service", mpr.appSpec.Service),
			zap.String("containerId", containerId),
		)
		return nil
	}

	return mpr.Delete(containerId)
}

// create 4 unit test
func (mpr *mapper) create(containerId string, value []byte) error {
	var ctrHb apputil.ContainerHeartbeat
	if err := json.Unmarshal(value, &ctrHb); err != nil {
		return errors.Wrap(err, string(value))
	}

	mpr.containerState.alive[containerId] = newTemporary(ctrHb.Timestamp)
	for _, shard := range ctrHb.Shards {
		mpr.shardState.alive[shard.Spec.Id] = newTemporary(ctrHb.Timestamp)
		mpr.shardState.alive[shard.Spec.Id].curContainerId = containerId
	}

	mpr.lg.Info(
		"state created",
		zap.String("service", mpr.appSpec.Service),
		zap.String("containerId", containerId),
		zap.String("timestamp", time.Unix(ctrHb.Timestamp, 0).String()),
		zap.Reflect("shards", ctrHb.Shards),
	)
	return nil
}

func (mpr *mapper) Delete(containerId string) error {
	mpr.mu.Lock()
	defer mpr.mu.Unlock()

	delete(mpr.containerState.alive, containerId)

	// TODO shard多的场景会慢，海量key的场景，单container存巨量shard的时候
	var shardIds []string
	for shardId, value := range mpr.shardState.alive {
		if value.curContainerId == containerId {
			delete(mpr.shardState.alive, shardId)
			shardIds = append(shardIds, shardId)
		}
	}

	mpr.lg.Info(
		"state deleted",
		zap.String("service", mpr.appSpec.Service),
		zap.String("containerId", containerId),
		zap.Strings("shardIds", shardIds),
	)
	return nil
}

func (mpr *mapper) Refresh(containerId string, event *clientv3.Event) error {
	mpr.mu.Lock()
	defer mpr.mu.Unlock()

	if event.Kv.Value == nil {
		mpr.lg.Warn(
			"empty value",
			zap.ByteString("key", event.Kv.Key),
			zap.Reflect("type", event.Type),
			zap.String("containerId", containerId),
		)
		return nil
	}

	var ctrHb apputil.ContainerHeartbeat
	if err := json.Unmarshal(event.Kv.Value, &ctrHb); err != nil {
		return errors.Wrap(err, string(event.Kv.Value))
	}

	// container
	mpr.containerState.alive[containerId] = newTemporary(ctrHb.Timestamp)
	tmpHbShardsMap := make(map[string]string)

	// shard 带有不合法lease的shard，不能认为存活，要触发rb，重新走drop和add
	// shardkeeper 的作用是尽可能传递合法shard
	for _, shard := range ctrHb.Shards {
		tmpHbShardsMap[shard.Spec.Id] = ""
		if shard.Spec.Lease.ID == mpr.shard.guardLeaseID || shard.Spec.Lease.ID == mpr.shard.bridgeLeaseID {
			t := newTemporary(ctrHb.Timestamp)
			t.curContainerId = containerId
			t.leaseID = shard.Spec.Lease.ID
			mpr.shardState.alive[shard.Spec.Id] = t

			mpr.lg.Info(
				"state shard refreshed",
				zap.String("service", mpr.appSpec.Service),
				zap.String("containerID", containerId),
				zap.String("shardID", shard.Spec.Id),
				zap.Int64("shardLeaseID", int64(shard.Spec.Lease.ID)),
			)
		} else {
			mpr.lg.Info(
				"found shard with invalid lease from container",
				zap.String("service", mpr.appSpec.Service),
				zap.String("containerID", containerId),
				zap.Reflect("shardLeaseID", shard.Spec.Lease),
				zap.Int64("guardLeaseID", int64(mpr.shard.guardLeaseID)),
			)
		}
	}

	// bug: shardkeeper重新启动，放弃所有当前的shard，保持container心跳时shards为null，导致sm的mapper没有清理掉内存shard，在rb时误判
	// 所以这里需要做差集，将心跳中不存在的shard从mapper中及时清理掉
	var dropShardIds []string
	for shardId, shard := range mpr.shardState.alive {
		if shard.curContainerId == containerId {
			if _, ok := tmpHbShardsMap[shardId]; !ok {
				delete(mpr.shardState.alive, shardId)
				dropShardIds = append(dropShardIds, shardId)
			}
		}
	}
	if len(dropShardIds) != 0 {
		mpr.lg.Info(
			"state shard refreshed, already be removed",
			zap.String("service", mpr.appSpec.Service),
			zap.String("containerID", containerId),
			zap.Strings("dropShardIds", dropShardIds),
		)
	}

	mpr.lg.Debug(
		"state refreshed",
		zap.String("service", mpr.appSpec.Service),
		zap.String("containerId", containerId),
		zap.Reflect("shards", ctrHb.Shards),
		zap.String("timestamp", time.Unix(ctrHb.Timestamp, 0).String()),
	)
	return nil
}

func (mpr *mapper) Wait(containerId string) error {
	mpr.mu.Lock()
	defer mpr.mu.Unlock()

	cur, ok := mpr.containerState.alive[containerId]
	if !ok {
		mpr.lg.Info(
			"not found in alive container",
			zap.String("service", mpr.appSpec.Service),
			zap.String("containerId", containerId),
		)
		return nil
	}

	// 判断是否需要等待一会再处理该事件，队列中的事件在第一个等待事件完结后，可能都已经达到需要被处理的时间点
	timeElapsed := time.Since(cur.lastHeartbeatTime)
	waitTime := mpr.maxRecoveryTime - timeElapsed
	if waitTime > 0 {
		mpr.lg.Info(
			"wait until timeout",
			zap.String("service", mpr.appSpec.Service),
			zap.String("containerId", containerId),
			zap.Duration("wait", waitTime),
			zap.Duration("maxRecoveryTime", mpr.maxRecoveryTime),
			zap.Duration("timeElapsed", timeElapsed),
		)
		time.Sleep(waitTime)
		mpr.lg.Info(
			"wait completed",
			zap.String("service", mpr.appSpec.Service),
			zap.String("containerId", containerId),
		)
	}
	return nil
}

type temporary struct {
	// lastHeartbeatTime 结合maxRecoveryTime控制事件的处理频率
	lastHeartbeatTime time.Time

	// curContainerId 针对shard场景，需要存储当前所属containerId，用于做rb
	curContainerId string

	// leaseID 表示当前shard的合法性
	leaseID clientv3.LeaseID
}

func newTemporary(t int64) *temporary {
	if t == 0 {
		return &temporary{lastHeartbeatTime: time.Now()}
	}
	return &temporary{lastHeartbeatTime: time.Unix(t, 0)}
}

type mapperState struct {
	alive map[string]*temporary
}

func newMapperState() *mapperState {
	return &mapperState{alive: make(map[string]*temporary)}
}

func (s *mapperState) ForEach(visitor func(id string, tmp *temporary) error) error {
	for id, tmp := range s.alive {
		if err := visitor(id, tmp); err != nil {
			return err
		}
	}
	return nil
}
