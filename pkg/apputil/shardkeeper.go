package apputil

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	// addTrigger 增加分片的事件类型
	addTrigger = "addTrigger"
	// dropTrigger 删除分片的事件类型
	dropTrigger = "dropTrigger"

	defaultSyncInterval = time.Second
)

// shardKeeper 参考raft中log replication节点的实现机制，记录日志到boltdb，开goroutine异步下发指令给调用方
type shardKeeper struct {
	lg *zap.Logger
	// container *Container

	// db 本次持久存储
	db *bolt.DB

	// trigger 从boltdb中提取的待提交内容，放入队列顺序执行，也可以并发(trigger有点硬用了)，算是对性能和可读性的优化吧
	trigger *evtrigger.Trigger

	stopper *GoroutineStopper

	// 以下字段从ShardServer初始化
	service   string
	shardImpl ShardInterface
	client    *etcdutil.EtcdClient
	session   *concurrency.Session

	// Unlock保证使用的相同mutex，否则myKey设定不上
	mu           sync.Mutex
	shardMutexes map[string]*concurrency.Mutex
}

type shardKeeperTriggerValue struct {
	// shardKeeperDbValue 携带db的数据，这里使用组合的形态
	shardKeeperDbValue

	shardId string
}

// shardKeeperDbValue 存储分片数据和管理信息
type shardKeeperDbValue struct {
	// Spec 分片基础信息
	Spec *ShardSpec `json:"spec"`

	// Disp 标记是否已经下发成功
	Disp bool `json:"disp"`

	// Drop 软删除，在异步协程中清理
	Drop bool `json:"drop"`
}

func (v *shardKeeperDbValue) String() string {
	b, _ := json.Marshal(v)
	return string(b)
}

func newShardKeeper(lg *zap.Logger, ss *ShardServer) (*shardKeeper, error) {
	sk := shardKeeper{
		lg:      lg,
		stopper: &GoroutineStopper{},

		service:   ss.Container().Service(),
		shardImpl: ss.opts.impl,
		client:    ss.Container().Client,
		session:   ss.Container().Session,

		shardMutexes: make(map[string]*concurrency.Mutex),
	}

	db, err := bolt.Open("shard.db", 0600, nil)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(sk.service))
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "")
	}
	sk.db = db

	tgr, _ := evtrigger.NewTrigger(
		evtrigger.WithLogger(lg),
		evtrigger.WithWorkerSize(1),
	)
	_ = tgr.Register(addTrigger, sk.Dispatch)
	_ = tgr.Register(dropTrigger, sk.Dispatch)
	sk.trigger = tgr

	// 基于boltdb当前内容做shard下发
	initFn := func(k, v []byte) error {
		var value shardKeeperDbValue
		if err := json.Unmarshal(v, &value); err != nil {
			return err
		}
		// 使用shardImpl还是lk.Add要区分清楚，forEach中如果有boltdb访问会block
		return sk.shardImpl.Add(string(k), value.Spec)
	}
	if err := sk.forEach(initFn); err != nil {
		return nil, errors.Wrap(err, "")
	}

	// 启动同步goroutine，对shard做move动作
	sk.stopper.Wrap(func(ctx context.Context) {
		TickerLoop(
			ctx,
			sk.lg,
			defaultSyncInterval,
			fmt.Sprintf("shardkeeper: service %s sync exit", sk.service),
			func(ctx context.Context) error {
				return sk.sync()
			},
		)
	})

	return &sk, nil
}

func (sk *shardKeeper) Add(id string, spec *ShardSpec) error {
	value := &shardKeeperDbValue{
		Spec: spec,
		Disp: false,
	}
	err := sk.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sk.service))
		return b.Put([]byte(id), []byte(value.String()))
	})
	return errors.Wrap(err, "")
}

func (sk *shardKeeper) Drop(id string) error {
	return sk.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sk.service))
		raw := b.Get([]byte(id))

		// 多次下发drop指令，发现boltdb中为nil，return ASAP
		if raw == nil {
			sk.lg.Warn(
				"drop shard again",
				zap.String("service", sk.service),
				zap.String("id", id),
			)
			sk.unlock(id)
			return nil
		}

		var dv shardKeeperDbValue
		if err := json.Unmarshal(raw, &dv); err != nil {
			return errors.Wrap(err, string(raw))
		}
		dv.Disp = false
		dv.Drop = true

		return errors.Wrap(b.Put([]byte(id), []byte(dv.String())), "")
	})
}

func (sk *shardKeeper) Load(id string) (string, error) {
	return sk.shardImpl.Load(id)
}

func (sk *shardKeeper) forEach(visitor func(k, v []byte) error) error {
	return sk.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sk.service))
		return b.ForEach(visitor)
	})
}

func (sk *shardKeeper) sync() error {
	syncFn := func(k, v []byte) error {
		var dv shardKeeperDbValue
		if err := json.Unmarshal(v, &dv); err != nil {
			return err
		}

		if dv.Disp {
			return nil
		}

		shardId := string(k)

		if dv.Drop {
			return sk.trigger.Put(
				&evtrigger.TriggerEvent{
					Key:   dropTrigger,
					Value: &shardKeeperTriggerValue{shardId: shardId},
				},
			)
		}

		return sk.trigger.Put(
			&evtrigger.TriggerEvent{
				Key:   addTrigger,
				Value: &shardKeeperTriggerValue{shardKeeperDbValue: dv, shardId: shardId},
			},
		)
	}
	return sk.forEach(syncFn)
}

func (sk *shardKeeper) Close() {
	sk.stopper.Close()
	sk.trigger.Close()
	_ = sk.db.Close()
	sk.lg.Info(
		"active closed",
		zap.String("service", sk.service),
	)
}

func (sk *shardKeeper) Dispatch(typ string, value interface{}) error {
	tv := value.(*shardKeeperTriggerValue)
	shardId := tv.shardId

	var opErr error
	switch typ {
	case addTrigger:
		if err := sk.lock(shardId); err != nil {
			return errors.Wrap(err, "")
		}

		// 有lock的前提下，下发boltdb中的分片给调用方，这里存在异常情况：
		// 1 lock失效，并已经下发给调用方，此处逻辑以boltdb中的shard为准，lock失效会触发shardKeeper的Close，
		spec := tv.Spec
		opErr = sk.shardImpl.Add(shardId, spec)
		if opErr == nil {
			// 下发成功后更新boltdb
			tv.Disp = true
			err := sk.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(sk.service))
				return b.Put([]byte(shardId), []byte(tv.String()))
			})
			if err != nil {
				return errors.Wrapf(err, "shardId: %s", shardId)
			}
			sk.lg.Info(
				"add ok",
				zap.String("typ", typ),
				zap.Reflect("tv", tv),
			)
			return nil
		}
	case dropTrigger:
		opErr = sk.shardImpl.Drop(shardId)
		if opErr == nil {
			if err := sk.unlock(shardId); err != nil {
				return errors.Wrap(err, "")
			}

			// 清理掉shard
			if err := sk.delete(shardId); err != nil {
				return errors.Wrapf(err, "shardId: %s", shardId)
			}
			sk.lg.Info(
				"drop ok",
				zap.String("typ", typ),
				zap.Reflect("tv", tv),
			)
			return nil
		}
	default:
		panic(fmt.Sprintf("unknown typ %s", typ))
	}

	sk.lg.Error(
		"op error, wait next round",
		zap.String("typ", typ),
		zap.Reflect("tv", tv),
		zap.Error(opErr),
	)

	return errors.Wrap(opErr, "")
}

func (sk *shardKeeper) delete(shardId string) error {
	return errors.Wrap(
		sk.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))
			return b.Delete([]byte(shardId))
		}),
		"",
	)
}

func (sk *shardKeeper) lock(shardId string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	lockPfx := EtcdPathAppShardHbId(sk.service, shardId)
	mutex := concurrency.NewMutex(sk.session, lockPfx)
	if err := mutex.Lock(sk.client.Client.Ctx()); err != nil {
		// lock被占用
		if err == concurrency.ErrLocked {
			// opt: 确认lock被占用，清理掉本地shard
			if err := sk.delete(shardId); err != nil {
				return errors.Wrapf(err, "shardId: %s", shardId)
			}

			// 确定被别的container占有
			sk.lg.Error(
				"lock occupied and deleted",
				zap.String("pfx", lockPfx),
				zap.Error(err),
			)
			return nil
		}
		// 上lock失败，可能有两种情况:
		// 1 etcd连接不上
		// 2 shard已经被占用
		// 无论哪种，等在这里就行，1的情况依赖etcd可用性
		return errors.Wrapf(err, "pfx: %s", lockPfx)
	}
	sk.shardMutexes[shardId] = mutex
	return nil
}

func (sk *shardKeeper) unlock(shardId string) error {
	sk.mu.Lock()
	defer sk.mu.Unlock()

	mu, ok := sk.shardMutexes[shardId]
	if ok {
		// unlock和删除node是两回事，mutex.Unlock调用一次后内部的myKey就失效，但是drop的goroutine和心跳的goroutine是异步的，在Unlock之后，还没有删除node之前，可能会触发heartbeat把节点加回来
		if _, err := sk.client.Delete(context.TODO(), mu.Key()); err != nil {
			return errors.Wrapf(err, "shardId %s", shardId)
		}
		sk.lg.Info(
			"unlock ok",
			zap.String("service", sk.service),
			zap.String("shardId", shardId),
		)
	}
	return nil
}
