package apputil

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	bolt "go.etcd.io/bbolt"
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

	// db 本次持久存储
	db *bolt.DB
	// service 作为bucket的key，区分度足够
	service string

	// shardImpl 接入方的分片实现
	shardImpl ShardInterface

	// trigger 从boltdb中提取的待提交内容，放入队列顺序执行，也可以并发(trigger有点硬用了)，算是对性能和可读性的优化吧
	trigger *evtrigger.Trigger

	stopper *GoroutineStopper
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

func newShardKeeper(lg *zap.Logger, service string, impl ShardInterface) (*shardKeeper, error) {
	lk := shardKeeper{
		lg:        lg,
		service:   service,
		shardImpl: impl,
		stopper:   &GoroutineStopper{},
	}

	db, err := bolt.Open("shard.db", 0600, nil)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(service))
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "")
	}
	lk.db = db

	tgr, _ := evtrigger.NewTrigger(
		evtrigger.WithLogger(lg),
		evtrigger.WithWorkerSize(1),
	)
	_ = tgr.Register(addTrigger, lk.Dispatch)
	_ = tgr.Register(dropTrigger, lk.Dispatch)
	lk.trigger = tgr

	// 基于boltdb当前内容做shard下发
	initFn := func(k, v []byte) error {
		var ss ShardSpec
		if err := json.Unmarshal(v, &ss); err != nil {
			return err
		}
		return lk.Add(string(k), &ss)
	}
	if err := lk.forEach(initFn); err != nil {
		return nil, errors.Wrap(err, "")
	}

	// 启动同步goroutine，对shard做move动作
	lk.stopper.Wrap(func(ctx context.Context) {
		TickerLoop(
			ctx,
			lk.lg,
			defaultSyncInterval,
			fmt.Sprintf("shardkeeper: service %s sync exit", lk.service),
			func(ctx context.Context) error {
				return lk.sync()
			},
		)
	})

	return &lk, nil
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
		var dv shardKeeperDbValue
		if err := json.Unmarshal(raw, &dv); err != nil {
			return err
		}
		dv.Disp = false
		dv.Drop = true

		return b.Put([]byte(id), []byte(dv.String()))
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

		if !dv.Disp {
			return nil
		}

		shardId := string(k)

		if dv.Drop {
			return sk.trigger.Put(
				&evtrigger.TriggerEvent{
					Key:   dropTrigger,
					Value: shardKeeperTriggerValue{shardId: shardId},
				},
			)
		}

		return sk.trigger.Put(
			&evtrigger.TriggerEvent{
				Key:   addTrigger,
				Value: shardKeeperTriggerValue{shardKeeperDbValue: dv, shardId: shardId},
			},
		)
	}
	return sk.forEach(syncFn)
}

func (sk *shardKeeper) Close() {
	sk.stopper.Close()
	sk.trigger.Close()
	_ = sk.db.Close()
}

func (sk *shardKeeper) Dispatch(typ string, value interface{}) error {
	tv := value.(*shardKeeperTriggerValue)
	shardId := tv.shardId

	var opErr error
	switch typ {
	case addTrigger:
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
				return err
			}
			sk.lg.Info(
				"add ok",
				zap.String("typ", typ),
				zap.String("shardId", shardId),
				zap.String("spec", spec.String()),
			)
			return nil
		}
	case dropTrigger:
		opErr = sk.shardImpl.Drop(shardId)
		if opErr == nil {
			// 下发成功后更新boltdb
			err := sk.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(sk.service))
				return b.Delete([]byte(shardId))
			})
			if err != nil {
				return err
			}
			sk.lg.Info(
				"drop ok",
				zap.String("typ", typ),
				zap.String("shardId", shardId),
			)
			return nil
		}
	default:
		panic(fmt.Sprintf("unknown typ %s", typ))
	}

	sk.lg.Error(
		"op error, wait next round",
		zap.String("typ", typ),
		zap.String("shardId", shardId),
		zap.Error(opErr),
	)

	return errors.Wrap(opErr, "")
}
