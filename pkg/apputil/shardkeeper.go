package apputil

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// rebalanceTrigger shardKeeper.rbTrigger 使用
	rebalanceTrigger = "rebalanceTrigger"

	// addTrigger shardKeeper.dispatchTrigger 使用
	addTrigger = "addTrigger"
	// dropTrigger shardKeeper.dispatchTrigger 使用
	dropTrigger = "dropTrigger"

	// defaultSyncInterval boltdb同步到app的周期
	defaultSyncInterval = 300 * time.Millisecond
)

var (
	noLease = &Lease{
		ID: clientv3.NoLease,
	}
)

type Lease struct {
	// ID rb在bridge和guard两个阶段grant的lease存储在这里，
	// 没有采用存在BridgeLeaseID和GuardLeaseID两个属性的设计，这种设计会导致逻辑使用ID的时候要有选择，没有侧重，随时需要了解所在rb的子阶段
	ID clientv3.LeaseID `json:"id"`

	// Expire 过期时间点，ID本身不是单调递增，LeaseID = memberID + timestamp + counter，如果etcd集群有变动，leaseID可能会减小，
	// leaseID 设计目的是全局唯一即可。
	// 1 leaseID的ttl在sm中如果要缩短，需要停集群超过历史最大leaseID ttl，回收所有历史leaseID，增大leaseID的ttl没有问题
	// 2 带有更新时间点的leaseID，会导致shard被放弃掉
	Expire int64 `json:"expire"`
}

func (l *Lease) BiggerThan(lease *Lease) bool {
	if l.Expire > 0 {
		return l.Expire > lease.Expire
	}
	// 兼容旧逻辑，保证etcd没有leader变更的情况且shard.db没有被写入的情况下，能够判断出lease的优先级
	return l.ID > lease.ID
}

func (l *Lease) EqualTo(lease *Lease) bool {
	return l.ID == lease.ID
}

func (l *Lease) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

type Assignment struct {
	// Drops v1版本只存放要干掉哪些，add仍旧由smserver在guard阶段下发
	Drops []string `json:"drops"`
}
type ShardLease struct {
	Lease

	// GuardLeaseID 不是 clientv3.NoLease ，代表是bridge阶段，且要求本地shard的lease属性是该值
	GuardLeaseID clientv3.LeaseID `json:"guardLeaseID"`

	// Assignment 包含本轮需要drop掉的shard
	Assignment *Assignment `json:"assignment"`
}

func (sl *ShardLease) String() string {
	b, _ := json.Marshal(sl)
	return string(b)
}

// shardKeeper 参考raft中log replication节点的实现机制，记录日志到boltdb，开goroutine异步下发指令给调用方
type shardKeeper struct {
	lg        *zap.Logger
	stopper   *GoroutineStopper
	service   string
	shardImpl ShardInterface
	client    etcdutil.EtcdWrapper

	// db 本次持久存储
	db *bolt.DB

	// rbTrigger rb事件log，按顺序单goroutine处理lease节点的event
	rbTrigger evtrigger.Trigger

	// dispatchTrigger shard move事件log，按顺序单goroutine提交给app
	// rbTrigger 的机制保证boltdb中存储的shard在不同节点上没有交集
	dispatchTrigger evtrigger.Trigger

	// initialized 第一次sync，需要无差别下发shard
	initialized bool

	// startRev 记录lease节点的rev，用于开启watch goroutine
	startRev int64
	// bridgeLease
	bridgeLease *Lease
	// guardLease acquireGuardLease 成功时才能赋值，直到下次rb
	guardLease *Lease
}

// ShardKeeperDbValue 存储分片数据和管理信息
type ShardKeeperDbValue struct {
	// Spec 分片基础信息
	Spec *ShardSpec `json:"spec"`

	// Disp 标记是否已经下发成功
	Disp bool `json:"disp"`

	// Drop 软删除，在异步协程中清理
	Drop bool `json:"drop"`
}

func (v *ShardKeeperDbValue) String() string {
	b, _ := json.Marshal(v)
	return string(b)
}

func newShardKeeper(lg *zap.Logger, c *Container) (*shardKeeper, error) {
	sk := shardKeeper{
		lg:        lg,
		stopper:   &GoroutineStopper{},
		service:   c.Service(),
		shardImpl: c.opts.impl,
		client:    c.Client,

		bridgeLease: noLease,
		guardLease:  noLease,
	}
	db, err := bolt.Open(filepath.Join(c.opts.shardDir, "shard.db"), 0600, nil)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if err := db.Update(
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(sk.service))
			return err
		},
	); err != nil {
		return nil, errors.Wrap(err, "")
	}
	sk.db = db

	sk.rbTrigger, _ = evtrigger.NewTrigger(
		evtrigger.WithLogger(lg),
		evtrigger.WithWorkerSize(1),
	)
	sk.rbTrigger.Register(rebalanceTrigger, sk.processRbEvent)
	sk.dispatchTrigger, _ = evtrigger.NewTrigger(
		evtrigger.WithLogger(lg),
		evtrigger.WithWorkerSize(1),
	)
	sk.dispatchTrigger.Register(addTrigger, sk.dispatch)
	sk.dispatchTrigger.Register(dropTrigger, sk.dispatch)

	// 标记本地shard的Disp为false，等待参与rb，或者通过guard lease对比直接参与
	if err := sk.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))
			return b.ForEach(
				func(k, v []byte) error {
					var dv ShardKeeperDbValue
					if err := json.Unmarshal(v, &dv); err != nil {
						return err
					}
					dv.Disp = false
					return b.Put(k, []byte(dv.String()))
				},
			)
		},
	); err != nil {
		sk.lg.Error(
			"Update error",
			zap.String("service", sk.service),
			zap.Error(err),
		)
		return nil, errors.Wrap(err, "")
	}

	leasePfx := EtcdPathAppLease(sk.service)
	gresp, err := sk.client.Get(context.Background(), leasePfx, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if gresp.Count == 0 {
		// 没有lease/guard节点，当前service没有被正确初始化
		sk.lg.Error(
			"guard lease not exist",
			zap.String("leasePfx", leasePfx),
			zap.Error(ErrNotExist),
		)
		return nil, errors.Wrap(ErrNotExist, "")
	}
	if gresp.Count == 1 {
		// 存在历史revision被compact的场景，所以可能watch不到最后一个event，这里通过get，防止miss event
		var lease ShardLease
		if err := json.Unmarshal(gresp.Kvs[0].Value, &lease); err != nil {
			return nil, errors.Wrap(err, "")
		}
		sk.guardLease = &lease.Lease
	}
	sk.startRev = gresp.Header.Revision + 1

	// 启动同步goroutine，对shard做move动作
	sk.stopper.Wrap(func(ctx context.Context) {
		TickerLoop(
			ctx,
			sk.lg,
			defaultSyncInterval,
			fmt.Sprintf("sync exit %s", sk.service),
			func(ctx context.Context) error {
				return sk.sync()
			},
		)
	})

	return &sk, nil
}

// watchLease 监听lease节点，及时参与到rb中
func (sk *shardKeeper) watchLease() {
	leasePfx := EtcdPathAppLease(sk.service)
	sk.stopper.Wrap(
		func(ctx context.Context) {
			WatchLoop(
				ctx,
				sk.lg,
				sk.client,
				leasePfx,
				sk.startRev,
				func(ctx context.Context, ev *clientv3.Event) error {
					return sk.rbTrigger.Put(
						&evtrigger.TriggerEvent{
							Key:   rebalanceTrigger,
							Value: ev,
						},
					)
				},
			)
		},
	)
}

func (sk *shardKeeper) processRbEvent(_ string, value interface{}) error {
	switch value.(type) {
	case *clientv3.Event:
		ev := value.(*clientv3.Event)
		key := string(ev.Kv.Key)

		lease, err := sk.parseLease(ev)
		if err != nil {
			sk.lg.Error(
				"parseLease error",
				zap.Error(err),
			)
			return err
		}
		sk.lg.Info(
			"receive rb event",
			zap.String("key", key),
			zap.Reflect("lease", lease),
			zap.Int32("type", int32(ev.Type)),
		)

		switch key {
		case EtcdPathAppBridge(sk.service):
			if err := sk.acquireBridgeLease(ev, lease); err != nil {
				sk.lg.Error(
					"acquireBridgeLease error",
					zap.String("key", key),
					zap.Reflect("lease", lease),
					zap.Error(err),
				)
				return err
			}
		case EtcdPathAppGuard(sk.service):
			if err := sk.acquireGuardLease(ev, lease); err != nil {
				sk.lg.Error(
					"acquireGuardLease error",
					zap.String("key", key),
					zap.Reflect("lease", lease),
					zap.Error(err),
				)
				return err
			}
		default:
			return errors.Errorf("unexpected key %s", key)
		}
	}
	return nil
}

func (sk *shardKeeper) parseLease(ev *clientv3.Event) (*ShardLease, error) {
	var value []byte
	if ev.Type == mvccpb.DELETE {
		value = ev.PrevKv.Value
	} else {
		value = ev.Kv.Value
	}
	var lease ShardLease
	if err := json.Unmarshal(value, &lease); err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &lease, nil
}

func (sk *shardKeeper) acquireBridgeLease(ev *clientv3.Event, lease *ShardLease) error {
	key := string(ev.Kv.Key)

	// bridge不存在修改场景
	if ev.IsModify() {
		err := errors.New("unexpected modify event")
		return errors.Wrap(err, "")
	}

	if ev.Type == mvccpb.DELETE {
		// delete事件，已经错过加入时机，需要回收掉和lease相关的所有shard，此处在rb中，
		// 是明确要drop掉所有shard的，理论上，所有bridge lease的shard都应该已经迁移到guard lease
		if err := sk.dropByLease(&lease.Lease); err != nil {
			return errors.Wrap(err, "")
		}
		sk.lg.Info(
			"drop bridge lease",
			zap.String("pfx", key),
			zap.Int64("lease", int64(lease.ID)),
		)
		return nil
	}

	sk.bridgeLease = noLease

	// 软删除
	// sync goroutine 提取db中待清理的shard，通过单独的trigger同步到app
	// 这块如果要确定app drop成功，可以有两个方案：
	// 1 先app drop，然后shard del
	// 2 先shard del，然后app drop
	// 上述方案在一致性会有点差异：
	// 1 db中保留shard，持续保持心跳，smserver可以认定shard没有被清理
	// 2 db中清理shard，但是app drop失败，会导致smserver认定shard被清理，但仍旧在工作，这种不一致不可接受
	// 所以1可以接受，但考虑到和app的耦合会造成过多问题，这里整体迁移到另一个trigger处理

	dropM := make(map[string]struct{})
	for _, drop := range lease.Assignment.Drops {
		dropM[drop] = struct{}{}
	}
	sk.lg.Info(
		"dropM",
		zap.String("key", key),
		zap.Int64("bridgeLease", int64(lease.ID)),
		zap.Reflect("dropM", dropM),
	)

	err := sk.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))
			return b.ForEach(
				func(k, v []byte) error {
					var dv ShardKeeperDbValue
					if err := json.Unmarshal(v, &dv); err != nil {
						return err
					}

					shardID := string(k)
					if _, ok := dropM[shardID]; ok {
						sk.lg.Info(
							"drop shard when acquire bridge",
							zap.String("shardID", shardID),
							zap.Reflect("v", dv),
							zap.Int64("bridgeLease", int64(lease.ID)),
						)
						// 软删除
						dv.Disp = false
						dv.Drop = true
					} else {
						if lease.GuardLeaseID == dv.Spec.Lease.ID {
							sk.lg.Info(
								"acquire bridge",
								zap.String("shardID", shardID),
								zap.Int64("GuardLeaseID", int64(lease.GuardLeaseID)),
								zap.Reflect("v", dv),
							)
							// 更新lease，相当于acquire bridge lease
							dv.Spec.Lease = &lease.Lease
						} else {
							// 持有过期guard lease的也直接干掉
							sk.lg.Info(
								"drop shard when acquire bridge, guard lease too old",
								zap.String("shardID", shardID),
								zap.Int64("GuardLeaseID", int64(lease.GuardLeaseID)),
								zap.Reflect("v", dv),
							)
							// 软删除
							dv.Disp = false
							dv.Drop = true
						}
					}

					return b.Put(k, []byte(dv.String()))
				},
			)
		},
	)
	if err != nil {
		return errors.Wrap(err, "")
	}
	sk.bridgeLease = &lease.Lease
	sk.lg.Info(
		"bridge: create success",
		zap.String("key", key),
		zap.Reflect("bridgeLease", lease),
		zap.Reflect("dropM", dropM),
	)
	return nil
}

func (sk *shardKeeper) acquireGuardLease(ev *clientv3.Event, lease *ShardLease) error {
	// guard处理创建场景，等待下一个event，smserver保证rb是由modify触发
	if ev.IsCreate() {
		err := errors.New("create event, wait until modify event")
		return errors.Wrap(err, "")
	}

	key := string(ev.Kv.Key)

	// 预先设定guardLease，boltdb的shard逐个过度到guardLease下
	sk.guardLease = &lease.Lease

	if sk.bridgeLease.EqualTo(noLease) {
		sk.lg.Warn(
			"guard: found bridge lease zero, do not not participating rb",
			zap.String("key", key),
			zap.Int64("guardLease", int64(lease.ID)),
		)
		return nil
	}
	defer func() {
		// 清理bridge，不管逻辑是否出错
		sk.bridgeLease = noLease
	}()

	// 每个shard的lease存在下面3种状态：
	// 1 shard的lease和guard lease相等，shard分配有效，什么都不用做
	// 2 shard拿着bridge lease，可以直接使用guard lease做更新，下次hb会带上给smserver
	// 3 shard没有bridge lease，shard分配无效，删除，应该只在节点挂掉一段时间后，才可能出现
	err := sk.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))

			return b.ForEach(
				func(k, v []byte) error {
					var value ShardKeeperDbValue
					if err := json.Unmarshal(v, &value); err != nil {
						return err
					}

					// app短暂重启
					if value.Spec.Lease.EqualTo(&lease.Lease) {
						sk.lg.Info(
							"lease valid",
							zap.Reflect("guard", lease.Lease),
							zap.String("shardId", value.Spec.Id),
						)

						// 4 debug
						if value.Drop {
							sk.lg.Warn(
								"unexpected drop, drop should only connect with bridgeLease",
								zap.Reflect("guard", lease.Lease),
								zap.String("shardId", value.Spec.Id),
							)
						}
						return nil
					}

					// guard和bridge有绑定关系，经过bridge才能guard
					if value.Spec.Lease.EqualTo(sk.bridgeLease) {
						sk.lg.Info(
							"lease valid",
							zap.Reflect("bridge", sk.bridgeLease),
							zap.Int64("guard", int64(lease.ID)),
							zap.String("shardId", value.Spec.Id),
						)
						value.Spec.Lease = &lease.Lease

						if value.Drop {
							sk.lg.Warn(
								"unexpected drop, drop should only connect with bridgeLease",
								zap.Int64("guard", int64(lease.ID)),
								zap.String("shardId", value.Spec.Id),
							)
						}
					} else {
						// 出现概率较少
						sk.lg.Warn(
							"lease too old",
							zap.Reflect("cur", value.Spec.Lease),
							zap.Reflect("guard", lease),
							zap.String("shardId", value.Spec.Id),
						)

						value.Disp = false
						value.Drop = true
					}

					if err := b.Put(k, []byte(value.String())); err != nil {
						return errors.Wrap(err, "")
					}
					return nil
				},
			)
		},
	)
	if err != nil {
		return errors.Wrap(err, "")
	}
	sk.lg.Info(
		"guard: update success",
		zap.String("key", key),
		zap.Reflect("guardLease", sk.guardLease),
	)
	return nil
}

func (sk *shardKeeper) dropByLease(lease *Lease) error {
	return sk.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))
			return b.ForEach(
				func(k, v []byte) error {
					var value ShardKeeperDbValue
					if err := json.Unmarshal(v, &value); err != nil {
						return err
					}

					// 要覆盖lease所有需要drop场景
					// 1 clientv3.NoLease server端把lease重置，无差别放弃本地的lease，后续新的lease不可能和本地的相同，人工做数据，或者etcd集群变更才会出现
					// 2 过期的lease一定要drop，不知道这个shard在其他节点的占用情况(分布式系统在有网络延时的情况下知道shard没人占用，意义不大)
					// 3 对于lease合法的情况，只有bridge场景下才可以drop。已经过渡到guard lease或者在rb中间加入的情况都可以drop
					if lease.ID == clientv3.NoLease || lease.BiggerThan(value.Spec.Lease) {
						value.Disp = false
						value.Drop = true
						if err := b.Put(k, []byte(value.String())); err != nil {
							return err
						}
					}
					return nil
				},
			)
		},
	)
}

func (sk *shardKeeper) Add(id string, spec *ShardSpec) error {
	value := &ShardKeeperDbValue{
		Spec: spec,
		Disp: false,
		Drop: false,
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
			return ErrNotExist
		}

		var dv ShardKeeperDbValue
		if err := json.Unmarshal(raw, &dv); err != nil {
			return errors.Wrap(err, string(raw))
		}
		dv.Disp = false
		dv.Drop = true

		return errors.Wrap(b.Put([]byte(id), []byte(dv.String())), "")
	})
}

func (sk *shardKeeper) forEachRead(visitor func(k, v []byte) error) error {
	return sk.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))
			return b.ForEach(visitor)
		},
	)
}

// sync 没有关注lease，boltdb中存在的就需要提交给app
func (sk *shardKeeper) sync() error {
	err := sk.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))
			return b.ForEach(
				func(k, v []byte) error {
					var dv ShardKeeperDbValue
					if err := json.Unmarshal(v, &dv); err != nil {
						sk.lg.Error(
							"json unmarshal shardKeeper value err, will drop",
							zap.String("v", string(v)),
							zap.Error(err),
						)
						return b.Delete(k)
					}

					// 已经下发且已经初始化才能返回
					if dv.Disp && sk.initialized {
						return nil
					}

					if dv.Drop {
						return sk.dispatchTrigger.Put(
							&evtrigger.TriggerEvent{
								Key:   dropTrigger,
								Value: &ShardKeeperDbValue{Spec: &ShardSpec{Id: dv.Spec.Id}},
							},
						)
					}

					// shard的lease一定和guardLease是相等的才可以下发
					if !dv.Spec.Lease.EqualTo(sk.guardLease) {
						sk.lg.Warn(
							"unexpected lease, wait for rb",
							zap.Reflect("dv", dv),
							zap.Reflect("guardLease", sk.guardLease),
						)
						return sk.dispatchTrigger.Put(
							&evtrigger.TriggerEvent{
								Key:   dropTrigger,
								Value: &ShardKeeperDbValue{Spec: &ShardSpec{Id: dv.Spec.Id}},
							},
						)
					}

					sk.lg.Info(
						"shard synchronizing",
						zap.String("service", sk.service),
						zap.Reflect("shard", dv),
					)

					return sk.dispatchTrigger.Put(
						&evtrigger.TriggerEvent{
							Key:   addTrigger,
							Value: &dv,
						},
					)
				},
			)
		},
	)

	if err != nil {
		return err
	}

	// 整体sync一遍，才进入运行时根据Disp属性选择同步状态
	if !sk.initialized {
		sk.initialized = true
	}
	return nil
}

func (sk *shardKeeper) dispatch(typ string, value interface{}) error {
	tv := value.(*ShardKeeperDbValue)
	shardId := tv.Spec.Id

	var opErr error
	switch typ {
	case addTrigger:
		// 有lock的前提下，下发boltdb中的分片给调用方，这里存在异常情况：
		// 1 lock失效，并已经下发给调用方，此处逻辑以boltdb中的shard为准，lock失效会触发shardKeeper的Close，
		opErr = sk.shardImpl.Add(shardId, tv.Spec)
		if opErr == nil || opErr == ErrExist {
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
				"app shard added",
				zap.String("typ", typ),
				zap.Reflect("tv", tv),
			)
			return nil
		}
	case dropTrigger:
		opErr = sk.shardImpl.Drop(shardId)
		if opErr == nil || opErr == ErrNotExist {
			// 清理掉shard
			err := sk.db.Update(
				func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte(sk.service))
					return b.Delete([]byte(shardId))
				},
			)
			if err != nil {
				return err
			}
			sk.lg.Info(
				"app shard dropped",
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

func (sk *shardKeeper) Close() {
	if sk.stopper != nil {
		sk.stopper.Close()
	}

	if sk.rbTrigger != nil {
		sk.rbTrigger.Close()
	}
	if sk.dispatchTrigger != nil {
		sk.dispatchTrigger.Close()
	}

	if sk.db != nil {
		sk.db.Close()
	}

	sk.lg.Info(
		"active closed",
		zap.String("service", sk.service),
	)
}
