package smserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	_ stateOps = new(mapperState)
)

const (
	// 通过key的区分，在UpdateState中区分情况处理，降低代码冗余
	containerTrigger = "containerTrigger"
	shardTrigger     = "shardTrigger"

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
	lg        *zap.Logger
	container *smContainer

	// appSpec 配置中有container单节点恢复阈值，影响当前service事件处理的方式
	appSpec         *smAppSpec
	maxRecoveryTime time.Duration

	mu sync.Mutex
	// containerState 存活container
	containerState *mapperState
	// shardState 存活shard
	shardState *mapperState

	// trigger 事件存储在内存中的队列里，逐一执行，尽量不卡在etcd，因为事件丢失是可恢复的
	trigger *evtrigger.Trigger

	// stopper 管理watch goroutine
	stopper *apputil.GoroutineStopper
}

func newMapper(lg *zap.Logger, container *smContainer, appSpec *smAppSpec) (*mapper, error) {
	mpr := mapper{
		lg:        lg,
		container: container,
		appSpec:   appSpec,
		stopper:   &apputil.GoroutineStopper{},
	}
	mpr.containerState = newMapperState(&mpr, containerTrigger)
	mpr.shardState = newMapperState(&mpr, shardTrigger)

	trigger, _ := evtrigger.NewTrigger(
		evtrigger.WithLogger(lg),
		evtrigger.WithWorkerSize(triggerWorkerSize),
	)
	mpr.trigger = trigger
	_ = mpr.trigger.Register(containerTrigger, mpr.UpdateState)
	_ = mpr.trigger.Register(shardTrigger, mpr.UpdateState)

	if mpr.appSpec.MaxRecoveryTime <= 0 {
		mpr.maxRecoveryTime = defaultMaxRecoveryTime
	}

	if err := mpr.initAndWatch(containerTrigger); err != nil {
		return nil, errors.Wrap(err, "")
	}
	if err := mpr.initAndWatch(shardTrigger); err != nil {
		return nil, errors.Wrap(err, "")
	}

	mpr.lg.Info(
		"mapper started",
		zap.String("service", mpr.appSpec.Service),
	)

	return &mpr, nil
}

func (lm *mapper) extractId(key string) string {
	// https://github.com/entertainment-venue/sm/commit/77c6ba8d36196b6fa5a115483083ae9777f70c7d
	// 目录结构引入mutex，导致有变化，id在倒数第二段
	arr := strings.Split(key, "/")
	str := arr[len(arr)-2]
	if str == "" {
		lm.lg.Panic(
			"key error",
			zap.String("key", key),
		)
	}
	return str
}

func (lm *mapper) initAndWatch(typ string) error {
	so := lm.getStateOps(typ)
	pfx := so.Prefix()
	getOpts := []clientv3.OpOption{clientv3.WithPrefix()}
	resp, err := lm.container.Client.Get(context.TODO(), pfx, getOpts...)
	if err != nil {
		return errors.Wrap(err, "")
	}

	for _, kv := range resp.Kvs {
		id := lm.extractId(string(kv.Key))
		if err := so.Create(id, kv.Value); err != nil {
			return errors.Wrap(err, "")
		}
	}
	startRev := resp.Header.Revision + 1

	lm.stopper.Wrap(
		func(ctx context.Context) {
			var watchOpts []clientv3.OpOption
			watchOpts = append(watchOpts, clientv3.WithPrefix())
			watchOpts = append(watchOpts, clientv3.WithRev(startRev))
			apputil.WatchLoop(
				ctx,
				lm.lg,
				lm.container.Client.Client,
				pfx,
				startRev,
				func(ctx context.Context, ev *clientv3.Event) error {
					// pkg中会先lock，然后再写入心跳内容
					if ev.Type != mvccpb.DELETE && ev.Kv.Value == nil {
						return nil
					}

					// 事件写入evtrigger，理论上不会漏事件，除非event不合法
					if err := lm.trigger.Put(&evtrigger.TriggerEvent{Key: typ, Value: ev}); err != nil {
						return errors.Wrap(err, "")
					}
					return nil
				},
			)
		},
	)
	lm.lg.Info(
		"watch hb",
		zap.String("pfx", pfx),
		zap.String("service", lm.appSpec.Service),
	)
	return nil
}

func (lm *mapper) AliveContainers() ArmorMap {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	r := make(ArmorMap)
	collectId := func(id string, tmp *temporary) error {
		r[id] = ""
		return nil
	}
	_ = lm.containerState.ForEach(collectId)
	return r
}

func (lm *mapper) AliveShards() map[string]*temporary {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	r := make(map[string]*temporary)
	collectId := func(id string, tmp *temporary) error {
		r[id] = tmp
		return nil
	}
	_ = lm.shardState.ForEach(collectId)
	return r
}

func (lm *mapper) Close() {
	if lm.stopper != nil {
		lm.stopper.Close()
	}
}

func (lm *mapper) UpdateState(key string, value interface{}) error {
	event := value.(*clientv3.Event)

	id := lm.extractId(string(event.Kv.Key))
	ops := lm.getStateOps(key)
	if event.IsCreate() || event.IsModify() {
		// 需要更新container或者shard的存活事件
		return ops.Refresh(id, event.Kv.Value)
	}

	if event.Type != mvccpb.DELETE {
		lm.lg.Error(
			"unexpected etcd event",
			zap.String("service", lm.appSpec.Service),
			zap.String("event", event.Kv.String()),
		)
		return nil
	}

	// container故障(短暂的网络、硬件问题等等)，或者重启
	if err := ops.Wait(id); err != nil {
		return err
	}

	// 通过后面的时间检测是否container已经回来，这块有两种方案：
	// 1. 轮询探测
	// 2. 便利evtrigger中的事件，如果有该container的put事件，直接return，否则删除掉
	var hasCreate bool
	findCreate := func(it interface{}) error {
		ev := it.(*clientv3.Event)
		if ev.IsCreate() {
			hasCreate = true
		} else if ev.Type == mvccpb.DELETE {
			hasCreate = false
		}
		return nil
	}
	_ = lm.trigger.ForEach(findCreate)
	if hasCreate {
		lm.lg.Info(
			"container recovery",
			zap.String("service", lm.appSpec.Service),
			zap.String("container", id),
		)
		return nil
	}

	return ops.Delete(id)
}

func (lm *mapper) getStateOps(key string) stateOps {
	var ops stateOps
	switch key {
	case shardTrigger:
		ops = lm.shardState
	case containerTrigger:
		ops = lm.containerState
	default:
		panic(fmt.Sprintf("unknown %s", key))
	}
	return ops
}

// stateOps threadsafe
type stateOps interface {
	// Refresh 心跳时，刷新内存数据
	Refresh(id string, d []byte) error

	Create(id string, value []byte) error

	// Delete 删除container或者shard
	Delete(id string) error

	// ForEach 遍历元素
	ForEach(func(id string, tmp *temporary) error) error

	// Wait delete事件的等待
	Wait(id string) error

	// Prefix 区分container或者shard，提供etcd节点
	Prefix() string
}

type temporary struct {
	// lastHeartbeatTime 结合maxRecoveryTime控制事件的处理频率
	lastHeartbeatTime time.Time

	// curContainerId 针对shard场景，需要存储当前所属containerId，用于做rb
	curContainerId string
}

func newTemporary(t int64) *temporary {
	if t == 0 {
		return &temporary{lastHeartbeatTime: time.Now()}
	}
	return &temporary{lastHeartbeatTime: time.Unix(t, 0)}
}

type mapperState struct {
	mpr   *mapper
	alive map[string]*temporary
	typ   string
}

func newMapperState(mpr *mapper, typ string) *mapperState {
	return &mapperState{
		mpr:   mpr,
		alive: make(map[string]*temporary),
		typ:   typ,
	}
}

// Create 初始化时使用，增加lock
func (s *mapperState) Create(id string, value []byte) error {
	s.mpr.mu.Lock()
	defer s.mpr.mu.Unlock()
	return s.create(id, value)
}

// create Refresh和Create都会使用，无lock
func (s *mapperState) create(id string, value []byte) error {
	switch s.typ {
	case shardTrigger:
		var t apputil.ShardHeartbeat
		if err := json.Unmarshal(value, &t); err != nil {
			return errors.Wrap(err, string(value))
		}
		s.alive[id] = newTemporary(t.Timestamp)
		s.alive[id].curContainerId = t.ContainerId
	default:
		var t apputil.Heartbeat
		if err := json.Unmarshal(value, &t); err != nil {
			return errors.Wrap(err, string(value))
		}
		s.alive[id] = newTemporary(t.Timestamp)
	}

	s.mpr.lg.Info(
		"state created",
		zap.String("service", s.mpr.appSpec.Service),
		zap.String("id", id),
		zap.String("typ", s.typ),
		zap.String("lastHeartbeatTime", s.alive[id].lastHeartbeatTime.String()),
	)
	return nil
}

func (s *mapperState) Delete(id string) error {
	s.mpr.mu.Lock()
	defer s.mpr.mu.Unlock()

	delete(s.alive, id)

	s.mpr.lg.Info(
		"state deleted",
		zap.String("service", s.mpr.appSpec.Service),
		zap.String("id", id),
		zap.String("typ", s.typ),
	)
	return nil
}

func (s *mapperState) Refresh(id string, d []byte) error {
	s.mpr.mu.Lock()
	defer s.mpr.mu.Unlock()

	cur, ok := s.alive[id]
	if !ok {
		// pkg中先lock心跳节点，然后再进行更新操作
		return errors.Wrap(s.create(id, d), "")
	}

	switch s.typ {
	case shardTrigger:
		var t apputil.ShardHeartbeat
		if err := json.Unmarshal(d, &t); err != nil {
			return errors.Wrap(err, "")
		}
		if t.Timestamp == 0 {
			cur.lastHeartbeatTime = time.Now()
		} else {
			cur.lastHeartbeatTime = time.Unix(t.Timestamp, 0)
		}
		cur.curContainerId = t.ContainerId
	default:
		var t apputil.Heartbeat
		if err := json.Unmarshal(d, &t); err != nil {
			return errors.Wrap(err, "")
		}
		if t.Timestamp == 0 {
			cur.lastHeartbeatTime = time.Now()
		} else {
			cur.lastHeartbeatTime = time.Unix(t.Timestamp, 0)
		}
	}

	s.mpr.lg.Debug(
		"state refreshed",
		zap.String("service", s.mpr.appSpec.Service),
		zap.String("id", id),
		zap.String("typ", s.typ),
		zap.String("lastHeartbeatTime", cur.lastHeartbeatTime.String()),
	)
	return nil
}

func (s *mapperState) ForEach(visitor func(id string, tmp *temporary) error) error {
	for id, tmp := range s.alive {
		if err := visitor(id, tmp); err != nil {
			return err
		}
	}
	return nil
}

func (s *mapperState) Wait(id string) error {
	s.mpr.mu.Lock()
	defer s.mpr.mu.Unlock()

	cur, ok := s.alive[id]
	if !ok {
		s.mpr.lg.Info(
			"not found",
			zap.String("service", s.mpr.appSpec.Service),
			zap.String("id", id),
			zap.String("typ", s.typ),
		)
		return nil
	}

	// 判断是否需要等待一会再处理该事件，队列中的事件在第一个等待事件完结后，可能都已经达到需要被处理的时间点
	timeElapsed := time.Since(cur.lastHeartbeatTime)
	waitTime := s.mpr.maxRecoveryTime - timeElapsed
	if waitTime > maxRecoveryWaitTime {
		s.mpr.lg.Warn(
			"maxRecoveryWaitTime exceeded",
			zap.String("service", s.mpr.appSpec.Service),
			zap.String("id", id),
			zap.Duration("maxRecoveryTime", s.mpr.maxRecoveryTime),
			zap.Duration("timeElapsed", time.Since(cur.lastHeartbeatTime)),
		)
		waitTime = maxRecoveryWaitTime
	}
	if waitTime > 0 {
		s.mpr.lg.Info(
			"wait until timeout",
			zap.String("service", s.mpr.appSpec.Service),
			zap.String("id", id),
			zap.Duration("maxRecoveryTime", s.mpr.maxRecoveryTime),
			zap.Duration("timeElapsed", time.Since(cur.lastHeartbeatTime)),
		)
		time.Sleep(waitTime)
		s.mpr.lg.Info(
			"wait completed",
			zap.String("service", s.mpr.appSpec.Service),
			zap.String("id", id),
		)
	}
	return nil
}

func (s *mapperState) Prefix() string {
	switch s.typ {
	case shardTrigger:
		return nodeAppShardHb(s.mpr.appSpec.Service)
	case containerTrigger:
		return nodeAppContainerHb(s.mpr.appSpec.Service)
	default:
		panic(fmt.Sprintf("unknown %s", s.typ))
	}
}
