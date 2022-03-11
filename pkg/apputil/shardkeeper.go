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
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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

type Assignment struct {
	// Drops v1版本只存放要干掉哪些，add仍旧由smserver在guard阶段下发
	Drops []string `json:"drops"`
}
type Lease struct {
	ID clientv3.LeaseID

	// Assignment 包含本轮需要drop掉的shard
	Assignment *Assignment `json:"assignment"`
}

func (l *Lease) String() string {
	b, _ := json.Marshal(l)
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
	rbTrigger *evtrigger.Trigger

	// dispatchTrigger shard move事件log，按顺序单goroutine提交给app
	// rbTrigger 的机制保证boltdb中存储的shard在不同节点上没有交集
	dispatchTrigger *evtrigger.Trigger

	// once sync方法goroutine只需要启动一次，在第一次完成参与到rb之后，否则db中存在历史数据，不能提交给app
	once sync.Once
	// initialized 第一次sync，需要无差别下发shard
	initialized bool

	bridgeLease clientv3.LeaseID
}

type sessionClosed struct {
	LeaseID clientv3.LeaseID
}

// shardKeeperDbValue 存储分片数据和管理信息
type shardKeeperDbValue struct {
	// ShardId 内容能够自描述
	ShardId string `json:"shardId"`

	// Spec 分片基础信息
	Spec *ShardSpec `json:"spec"`

	// Disp 标记是否已经下发成功
	Disp bool `json:"disp"`

	// Drop 软删除，在异步协程中清理
	Drop bool `json:"drop"`

	// Lease 当前lease
	Lease clientv3.LeaseID `json:"lease"`
}

func (v *shardKeeperDbValue) String() string {
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
	}

	db, err := bolt.Open("shard.db", 0600, nil)
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

	leasePfx := EtcdPathAppLease(sk.service)
	gresp, err := sk.client.Get(context.TODO(), leasePfx, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	if gresp.Count == 1 {
		key := string(gresp.Kvs[0].Key)
		if key != EtcdPathAppGuard(sk.service) {
			err := errors.Errorf("unexpected key %s", key)
			sk.lg.Error(
				"unexpected key",
				zap.String("key", key),
				zap.Error(err),
			)
			return nil, errors.Wrap(err, "")
		}

		var lease Lease
		if err := json.Unmarshal(gresp.Kvs[0].Value, &lease); err != nil {
			sk.lg.Error(
				"Unmarshal error",
				zap.String("key", key),
				zap.Error(err),
			)
			return nil, errors.Wrap(err, "")
		}

		// 根据guard lease初始化当前环境
		if err := sk.tryGuardLease(&lease); err != nil {
			sk.lg.Error(
				"tryGuardLease error",
				zap.String("key", key),
				zap.Error(err),
			)
		}
	}
	// 处理lease节点最后一个event
	sk.watchLease(gresp.Header.Revision)

	return &sk, nil
}

func (sk *shardKeeper) tryGuardLease(lease *Lease) error {
	return sk.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))

			return b.ForEach(
				func(k, v []byte) error {
					var value shardKeeperDbValue
					if err := json.Unmarshal(v, &value); err != nil {
						return err
					}

					if value.Lease != lease.ID {
						return nil
					}

					sk.lg.Info(
						"lease valid, mark shard for dispatching",
						zap.Int64("guard", int64(lease.ID)),
						zap.Reflect("value", value),
					)

					// 下发给app，再通过watchLease修正
					value.Disp = false
					value.Drop = false
					return b.Put(k, []byte(value.String()))
				},
			)
		},
	)
}

// watchLease 监听lease节点，及时参与到rb中
func (sk *shardKeeper) watchLease(startRev int64) {
	leasePfx := EtcdPathAppLease(sk.service)
	sk.stopper.Wrap(
		func(ctx context.Context) {
			WatchLoop(
				ctx,
				sk.lg,
				sk.client,
				leasePfx,
				startRev,
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
	case *sessionClosed:
		sk.lg.Info(
			"receive sessionClosed",
			zap.Reflect("value", value),
		)
		// session关闭，相关shard都需要drop掉
		v := value.(*sessionClosed)
		if err := sk.dropLease(v.LeaseID, false); err != nil {
			return err
		}
	case *clientv3.Event:
		ev := value.(*clientv3.Event)
		key := string(ev.Kv.Key)

		// TODO 确认bridge删除事件，是否存在value，如果存在，下面的session理论上应该失败
		var value []byte
		if ev.Type == mvccpb.DELETE {
			value = ev.PrevKv.Value
		} else {
			value = ev.Kv.Value
		}
		var lease Lease
		if err := json.Unmarshal(value, &lease); err != nil {
			sk.lg.Error(
				"Unmarshal error",
				zap.String("pfx", key),
				zap.ByteString("value", ev.Kv.Value),
				zap.Error(err),
			)
			return err
		}

		sk.lg.Info(
			"receive rb event",
			zap.String("key", key),
			zap.ByteString("value", value),
		)

		var session *concurrency.Session
		if ev.Type != mvccpb.DELETE {
			// 构建session，确认lease合法
			var err error
			session, err = concurrency.NewSession(sk.client.GetClient().Client, concurrency.WithLease(lease.ID))
			if err != nil {
				sk.lg.Error(
					"NewSession error",
					zap.String("pfx", key),
					zap.Reflect("lease", lease),
					zap.Error(err),
				)
				return err
			}
			sk.lg.Info(
				"session created",
				zap.String("key", key),
				zap.Int64("LeaseID", int64(lease.ID)),
			)
		}

		switch key {
		case EtcdPathAppBridge(sk.service):
			// bridge不存在修改场景
			if ev.IsModify() {
				err := errors.New("unexpected modify event")
				sk.lg.Error(
					err.Error(),
					zap.String("pfx", key),
					zap.Reflect("lease", lease),
				)
				return err
			}

			sk.bridgeLease = clientv3.NoLease

			if ev.Type == mvccpb.DELETE {
				// delete事件，已经错过加入时机，需要回收掉和lease相关的所有shard
				if err := sk.dropLease(lease.ID, true); err != nil {
					return err
				}
				sk.lg.Info(
					"drop bridge lease",
					zap.String("pfx", key),
					zap.Int64("lease", int64(lease.ID)),
				)
				return nil
			}

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

			err := sk.db.Update(
				func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte(sk.service))
					return b.ForEach(
						func(k, v []byte) error {
							var value shardKeeperDbValue
							if err := json.Unmarshal(v, &value); err != nil {
								return err
							}

							var dv shardKeeperDbValue
							if err := json.Unmarshal(v, &dv); err != nil {
								return err
							}

							if _, ok := dropM[string(k)]; ok {
								sk.lg.Info(
									"drop shard when acquire bridge",
									zap.String("id", string(k)),
								)
								// 软删除
								dv.Disp = false
								dv.Drop = true
							} else {
								sk.lg.Info(
									"acquire bridge",
									zap.String("id", string(k)),
								)
								// 更新lease，相当于acquire bridge lease
								dv.Lease = lease.ID
							}

							return b.Put(k, []byte(dv.String()))
						},
					)
				},
			)
			if err != nil {
				sk.lg.Error(
					"bridge: update shard error",
					zap.String("key", key),
					zap.Int64("LeaseID", int64(lease.ID)),
					zap.Reflect("dropM", dropM),
					zap.Error(err),
				)
				return err
			}
			sk.lg.Info(
				"bridge: update shard succ",
				zap.String("key", key),
				zap.Int64("LeaseID", int64(lease.ID)),
				zap.Reflect("dropM", dropM),
			)
			sk.bridgeLease = lease.ID

		case EtcdPathAppGuard(sk.service):
			// guard处理创建场景，等待下一个event，smserver保证rb是由modify触发
			if ev.IsCreate() {
				err := errors.New("create event, wait until modify event")
				sk.lg.Warn(
					err.Error(),
					zap.String("pfx", key),
					zap.Reflect("lease", lease),
				)
				return err
			}

			// 每个shard的lease存在下面3种状态：
			// 1 shard的lease和guard lease相等，shard分配有效，什么都不用做
			// 2 shard拿着bridge lease，可以直接使用guard lease做更新，下次hb会带上给smserver
			// 3 shard没有bridge lease，shard分配无效，删除，应该只在节点挂掉一段时间后，才可能出现
			err := sk.db.Update(
				func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte(sk.service))

					return b.ForEach(
						func(k, v []byte) error {
							var value shardKeeperDbValue
							if err := json.Unmarshal(v, &value); err != nil {
								return err
							}

							if value.Lease == lease.ID {
								sk.lg.Info(
									"lease valid",
									zap.Int64("guard", int64(lease.ID)),
									zap.String("shardId", value.ShardId),
								)
								return nil
							}

							if value.Lease == sk.bridgeLease {
								sk.lg.Info(
									"lease valid",
									zap.Int64("bridge", int64(sk.bridgeLease)),
									zap.Int64("guard", int64(lease.ID)),
									zap.String("shardId", value.ShardId),
								)
								value.Lease = lease.ID
							} else {
								// 出现概率较少
								sk.lg.Warn(
									"lease too old",
									zap.Int64("cur", int64(value.Lease)),
									zap.Int64("guard", int64(lease.ID)),
									zap.String("shardId", value.ShardId),
								)

								value.Disp = false
								value.Drop = true
							}

							if err := b.Put(k, []byte(value.String())); err != nil {
								return err
							}
							return nil
						},
					)
				},
			)
			if err != nil {
				sk.lg.Error(
					"guard: update shard error",
					zap.String("key", key),
					zap.Int64("LeaseID", int64(lease.ID)),
					zap.Error(err),
				)
				return err
			}
			sk.lg.Info(
				"guard: update shard succ",
				zap.String("key", key),
				zap.Int64("LeaseID", int64(lease.ID)),
			)
		default:
			err := errors.Errorf("unexpected key %s", key)
			return err
		}

		// 关注session的存活状态
		// 异常情况下回收掉没有acquire guard lease的shard
		// TODO 事件处理成功后，开启低session的关注，如果session在事件处理期间失败，不确定session.Done是否能监测到
		if ev.Type != mvccpb.DELETE {
			sk.stopper.Wrap(
				func(ctx context.Context) {
					select {
					case <-ctx.Done():
						// 主动关闭 退出goroutine即可
						sk.lg.Info(
							"session active exit",
							zap.String("pfx", key),
							zap.Reflect("lease", lease),
						)
					case <-session.Done():
						sk.lg.Info(
							"session passive closed",
							zap.String("pfx", key),
							zap.Reflect("lease", lease),
						)

						// 要回收掉当前lease相关的所有shard，出现的情况可能有：
						// 1 新的guard把shard已经接管走
						// 2 session异常退出，相关shard需要停下来
						sk.rbTrigger.Put(
							&evtrigger.TriggerEvent{
								Key:   rebalanceTrigger,
								Value: &sessionClosed{LeaseID: lease.ID},
							},
						)
					}
				},
			)
		}

		// 成功刷新一次shard的lease之后，启动同步goroutine
		sk.once.Do(
			func() {
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
			},
		)
	}
	return nil
}

func (sk *shardKeeper) dropLease(leaseID clientv3.LeaseID, ignoreEqualCase bool) error {
	return sk.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))

			return b.ForEach(
				func(k, v []byte) error {
					var value shardKeeperDbValue
					if err := json.Unmarshal(v, &value); err != nil {
						return err
					}

					if value.Lease < leaseID || (ignoreEqualCase && value.Lease == leaseID) {
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
	value := &shardKeeperDbValue{
		Spec:  spec,
		Disp:  false,
		Lease: spec.LeaseID,
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

func (sk *shardKeeper) forEachRead(visitor func(k, v []byte) error) error {
	return sk.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(sk.service))
			return b.ForEach(visitor)
		},
	)
}

func (sk *shardKeeper) sync() error {
	err := sk.forEachRead(
		func(k, v []byte) error {
			var dv shardKeeperDbValue
			if err := json.Unmarshal(v, &dv); err != nil {
				return err
			}

			if dv.Disp && sk.initialized {
				return nil
			}

			shardId := string(k)

			if dv.Drop {
				return sk.dispatchTrigger.Put(
					&evtrigger.TriggerEvent{
						Key:   dropTrigger,
						Value: &shardKeeperDbValue{ShardId: shardId},
					},
				)
			}

			dv.ShardId = shardId

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
	tv := value.(*shardKeeperDbValue)
	shardId := tv.ShardId

	var opErr error
	switch typ {
	case addTrigger:
		// 有lock的前提下，下发boltdb中的分片给调用方，这里存在异常情况：
		// 1 lock失效，并已经下发给调用方，此处逻辑以boltdb中的shard为准，lock失效会触发shardKeeper的Close，
		spec := tv.Spec
		opErr = sk.shardImpl.Add(shardId, spec)
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
				"dispatch add ok",
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
				"dispatch drop ok",
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
