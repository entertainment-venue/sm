package apputil

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/pkg/errors"
	"github.com/zd3tl/evtrigger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// rebalanceTrigger shardKeeper.rbTrigger 使用
	rebalanceTrigger = "rebalanceTrigger"

	// defaultSyncInterval boltdb同步到app的周期
	defaultSyncInterval = 300 * time.Millisecond
)

type Assignment struct {
	// Drops v1版本只存放要干掉哪些，add仍旧由smserver在guard阶段下发
	Drops []string `json:"drops"`
}
type ShardLease struct {
	storage.Lease

	// GuardLeaseID 不是clientv3.NoLease ，代表是bridge阶段，且要求本地shard的lease属性是该值
	GuardLeaseID clientv3.LeaseID `json:"guardLeaseID"`

	// BridgeLeaseID 在guard阶段，要求本地的bridge是这个值
	BridgeLeaseID clientv3.LeaseID `json:"bridgeLeaseID"`

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

	containerId string

	// storage 持久存储
	storage storage.Storage

	// rbTrigger rb事件log，按顺序单goroutine处理lease节点的event
	rbTrigger evtrigger.Trigger

	// initialized 第一次sync，需要无差别下发shard
	initialized bool

	// startRev 记录lease节点的rev，用于开启watch goroutine
	startRev int64
	// bridgeLease acquireBridgeLease 赋值，当前bridge lease，rb阶段只需要拿到第一次通过etcd下发的lease，没有lease续约的动作
	bridgeLease *storage.Lease
	// guardLease acquireGuardLease 赋值，当前guard lease，成功时才能赋值，直到下次rb
	guardLease *storage.Lease

	// dropExpiredShard 默认false，分片应用明确决定对lease敏感，才开启
	dropExpiredShard bool
}

func newShardKeeper(lg *zap.Logger, c *Container) (*shardKeeper, error) {
	sk := shardKeeper{
		lg:        lg,
		stopper:   &GoroutineStopper{},
		service:   c.Service(),
		shardImpl: c.opts.impl,
		client:    c.Client,

		containerId: c.Id(),

		bridgeLease: storage.NoLease,
		guardLease:  storage.NoLease,

		dropExpiredShard: c.opts.dropExpiredShard,
	}

	var err error
	switch c.opts.storageType {
	case storage.Etcd:
		sk.storage, err = storage.NewEtcddb(sk.service, sk.containerId, sk.client, sk.lg)
	default:
		sk.storage, err = storage.NewBoltdb(c.opts.shardDir, sk.service, sk.lg)
	}
	if err != nil {
		return nil, err
	}

	sk.rbTrigger, _ = evtrigger.NewTrigger(evtrigger.WithLogger(lg), evtrigger.WithWorkerSize(1))
	sk.rbTrigger.Register(rebalanceTrigger, sk.handleRbEvent)

	// 标记本地shard的Disp为false，等待参与rb，或者通过guard lease对比直接参与
	if err := sk.storage.Reset(); err != nil {
		sk.lg.Error(
			"Reset error",
			zap.String("service", sk.service),
			zap.Error(err),
		)
		return nil, err
	}

	leasePfx := etcdutil.LeasePath(sk.service)
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
		if !sk.dropExpiredShard {
			// 默认client不开启，降低client对lease的敏感度：
			// 1 server长时间不刷新lease
			// 2 网络问题或etcd异常会导致lease失效
			// 以上场景，会导致shard停止服务，不开启的情况下，会使用旧的guard lease继续运行
			sk.guardLease = &lease.Lease

			sk.lg.Info(
				"ignore guard lease expire status",
				zap.String("service", sk.service),
				zap.Int64("guard-lease", int64(lease.ID)),
			)
		} else {
			// 判断lease的合法性，expire后续会废弃掉，统一通过etcd做lease合法性校验
			clientv3Lease := clientv3.NewLease(sk.client.GetClient().Client)
			ctx, cancel := context.WithTimeout(context.TODO(), etcdutil.DefaultRequestTimeout)
			res, err := clientv3Lease.TimeToLive(ctx, lease.ID)
			cancel()
			if err != nil {
				sk.lg.Error(
					"guard lease fetch error",
					zap.String("service", sk.service),
					zap.Int64("guard-lease", int64(lease.ID)),
					zap.Error(err),
				)
			} else {
				if res.TTL <= 0 {
					sk.lg.Warn(
						"guard lease expired, shards will be dropped",
						zap.String("service", sk.service),
						zap.Int64("guard-lease", int64(lease.ID)),
					)
				} else {
					sk.guardLease = &lease.Lease
					sk.lg.Info(
						"guard lease not expired",
						zap.String("service", sk.service),
						zap.Int64("guard-lease", int64(lease.ID)),
					)
				}
			}
		}
	}
	sk.startRev = gresp.Header.Revision + 1

	// 启动同步goroutine，对shard做move动作
	sk.stopper.Wrap(func(ctx context.Context) {
		SequenceTickerLoop(
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

// WatchLease 监听lease节点，及时参与到rb中
func (sk *shardKeeper) WatchLease() {
	leasePfx := etcdutil.LeasePath(sk.service)
	sk.stopper.Wrap(
		func(ctx context.Context) {
			WatchLoop(
				ctx,
				sk.lg,
				sk.client,
				leasePfx,
				sk.startRev,
				func(ctx context.Context, ev *clientv3.Event) error {
					return sk.rbTrigger.Put(&evtrigger.TriggerEvent{Key: rebalanceTrigger, Value: ev})
				},
			)
		},
	)
}

func (sk *shardKeeper) handleRbEvent(_ string, value interface{}) error {
	ev, ok := value.(*clientv3.Event)
	if !ok {
		return errors.New("type error")
	}
	key := string(ev.Kv.Key)

	lease, err := sk.parseShardLease(ev)
	if err != nil {
		sk.lg.Error(
			"parseShardLease error",
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
	case etcdutil.LeaseBridgePath(sk.service):
		if err := sk.acquireBridgeLease(ev, lease); err != nil {
			sk.lg.Error(
				"acquireBridgeLease error",
				zap.String("key", key),
				zap.Reflect("lease", lease),
				zap.Error(err),
			)
			return nil
		}
	case etcdutil.LeaseGuardPath(sk.service):
		if err := sk.acquireGuardLease(ev, lease); err != nil {
			sk.lg.Error(
				"acquireGuardLease error",
				zap.String("key", key),
				zap.Reflect("lease", lease),
				zap.Error(err),
			)
			return nil
		}
	default:
		if !strings.HasPrefix(key, etcdutil.LeaseSessionDir(sk.service)) {
			return errors.Errorf("unexpected key [%s]", key)
		}
		return sk.handleSessionKeyEvent(ev)
	}
	return nil
}

func (sk *shardKeeper) parseShardLease(ev *clientv3.Event) (*ShardLease, error) {
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

func (sk *shardKeeper) handleSessionKeyEvent(ev *clientv3.Event) error {
	switch ev.Type {
	case mvccpb.PUT:
		k := ev.Kv.Key
		v := ev.Kv.Value
		if ev.IsCreate() {
			sk.lg.Info(
				"lease session receive create event, ignore",
				zap.String("service", sk.service),
				zap.ByteString("key", k),
				zap.ByteString("value", v),
			)
			return nil
		}
		if ev.IsModify() {
			sk.lg.Info(
				"lease session receive modify event, ignore",
				zap.String("service", sk.service),
				zap.ByteString("key", ev.Kv.Key),
				zap.ByteString("value", ev.Kv.Value),
			)
			return nil
		}
	case mvccpb.DELETE:
		k := ev.PrevKv.Key
		v := ev.PrevKv.Value
		var lease storage.Lease
		if err := json.Unmarshal(v, &lease); err != nil {
			panic(fmt.Sprintf("key [%s] receive delete event, Unmarshal error [%s] with value [%s]", string(k), err.Error(), string(v)))
		}
		if err := sk.storage.DropByLease(false, lease.ID); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("unexpected event type [%s] for lease [%s] at [%s]", ev.Type, string(ev.Kv.Value), string(ev.Kv.Key)))
	}
	return nil
}

func (sk *shardKeeper) acquireBridgeLease(ev *clientv3.Event, lease *ShardLease) error {
	key := string(ev.Kv.Key)

	// bridge不存在修改场景
	if ev.IsModify() {
		err := errors.Errorf("unexpected modify event, key %s", string(ev.Kv.Key))
		return errors.Wrap(err, "")
	}

	if ev.Type == mvccpb.DELETE {
		if err := sk.storage.DropByLease(false, lease.ID); err != nil {
			return err
		}
		sk.lg.Info(
			"drop bridge lease completed",
			zap.String("pfx", key),
			zap.Int64("lease", int64(lease.ID)),
		)
		return nil
	}

	// reset bridge lease，清除 shardKeeper 当前的临时变量，方便开启新的rb
	sk.bridgeLease = storage.NoLease

	if err := sk.storage.Drop(lease.Assignment.Drops); err != nil {
		return err
	}

	if err := sk.storage.MigrateLease(lease.GuardLeaseID, lease.ID); err != nil {
		return err
	}

	sk.bridgeLease = &lease.Lease
	sk.lg.Info(
		"bridge: create success",
		zap.String("key", key),
		zap.Reflect("bridge-lease", lease),
	)
	return nil
}

func (sk *shardKeeper) acquireGuardLease(ev *clientv3.Event, lease *ShardLease) error {
	// guard处理创建场景，等待下一个event，smserver保证rb是由modify触发
	if ev.IsCreate() {
		return errors.Errorf(
			"guard node should be created before shardkeeper started, key [%s]",
			string(ev.Kv.Key),
		)
	}

	key := string(ev.Kv.Key)

	// 非renew场景，肯定是在rb中，所以当前的bridgeLease需要存在值
	if sk.bridgeLease.EqualTo(storage.NoLease) {
		return errors.Errorf(
			"bridge lease is zero, can not participating rb, key [%s]",
			key,
		)
	}

	// 兼容现存的rb提供的lease信息
	if lease.BridgeLeaseID > 0 && sk.bridgeLease.ID != lease.BridgeLeaseID {
		return errors.Errorf(
			"bridge lease not match, key [%s] expect [%d] actual [%d]",
			string(ev.Kv.Key),
			lease.BridgeLeaseID,
			sk.bridgeLease.ID,
		)
	}

	defer func() {
		// 清理bridge，不管逻辑是否出错
		sk.bridgeLease = storage.NoLease
	}()

	// 预先设定guardLease，boltdb的shard逐个过度到guardLease下
	sk.guardLease = &lease.Lease

	// 每个shard的lease存在下面3种状态：
	// 1 shard的lease和guard lease相等，shard分配有效，什么都不用做
	// 2 shard拿着bridge lease，可以直接使用guard lease做更新，下次hb会带上给smserver
	// 3 shard没有bridge lease，shard分配无效，删除，应该只在节点挂掉一段时间后，才可能出现
	if err := sk.storage.MigrateLease(sk.bridgeLease.ID, lease.ID); err != nil {
		return err
	}

	if err := sk.storage.DropByLease(true, lease.ID); err != nil {
		return err
	}

	sk.lg.Info(
		"guard lease update success",
		zap.String("key", key),
		zap.Reflect("guardLease", sk.guardLease),
	)

	// 存储和lease的关联节点
	sessionPath := etcdutil.LeaseSessionPath(sk.service, sk.containerId)
	leaseIDStr := strconv.FormatInt(int64(sk.guardLease.ID), 10)
	if _, err := sk.client.Put(context.TODO(), sessionPath, sk.guardLease.String(), clientv3.WithLease(sk.guardLease.ID)); err != nil {
		sk.lg.Error(
			"Put error",
			zap.String("session-path", sessionPath),
			zap.String("guard-lease-id", leaseIDStr),
			zap.Error(err),
		)
	}

	return nil
}

func (sk *shardKeeper) Add(id string, spec *storage.ShardSpec) error {
	// 提前判断添加shard场景下的细节，让storage内部逻辑尽量明确
	if !spec.Lease.EqualTo(sk.guardLease) {
		sk.lg.Warn(
			"shard guard lease not equal with guard lease",
			zap.String("service", sk.service),
			zap.String("shard-id", id),
			zap.Int64("local-guard-lease", int64(sk.guardLease.ID)),
			zap.Int64("shard-guard-lease", int64(spec.Lease.ID)),
		)
		return errors.New("lease mismatch")
	}
	return sk.storage.Add(spec)
}

func (sk *shardKeeper) Drop(id string) error {
	return sk.storage.Drop([]string{id})
}

// sync 没有关注lease，boltdb中存在的就需要提交给app
func (sk *shardKeeper) sync() error {
	var (
		dropShardIDs   []string
		updateDbValues = make(map[string]*storage.ShardKeeperDbValue)
	)

	dropFn := func(dv *storage.ShardKeeperDbValue) error {
		err := sk.shardImpl.Drop(dv.Spec.Id)
		if err == nil || err == ErrNotExist {
			// 清理掉shard
			dropShardIDs = append(dropShardIDs, dv.Spec.Id)
			return nil
		}
		sk.lg.Error(
			"drop shard failed",
			zap.String("service", sk.service),
			zap.String("shardId", dv.Spec.Id),
			zap.Error(err),
		)
		return err
	}

	addFn := func(dv *storage.ShardKeeperDbValue) error {
		err := sk.shardImpl.Add(dv.Spec.Id, dv.Spec)
		if err == nil || err == ErrExist {
			// 下发成功后更新boltdb
			dv.Disp = true
			updateDbValues[dv.Spec.Id] = dv
			return nil
		}
		sk.lg.Error(
			"add shard failed",
			zap.String("service", sk.service),
			zap.String("shardId", dv.Spec.Id),
			zap.Error(err),
		)
		return err
	}

	sk.storage.ForEach(func(shardID string, dv *storage.ShardKeeperDbValue) error {
		// shard的lease一定和guardLease是相等的才可以下发
		/*
			这种要求shardkeeper下发shard的情况，有两个通道：
			1. 从http add请求
			2. watch lease，发现需要drop（不会走到问题逻辑）
			1这种情况，sm在guardlease的更新和http请求下发之间停10s，等待client同步，然后下发，如果10s这个问题client都没同步到最新的guardlease，drop即可
		*/
		if !dv.Spec.Lease.EqualTo(sk.guardLease) && !dv.Spec.Lease.EqualTo(sk.bridgeLease) {
			sk.lg.Warn(
				"unexpected lease, will be dropped",
				zap.Reflect("dv", dv),
				zap.Reflect("guardLease", sk.guardLease),
			)
			return dropFn(dv)
		}

		if dv.Disp && sk.initialized {
			return nil
		}

		if dv.Drop {
			sk.lg.Info(
				"drop shard from app",
				zap.String("service", sk.service),
				zap.Reflect("shard", dv),
			)
			return dropFn(dv)
		}

		sk.lg.Info(
			"add shard to app",
			zap.String("service", sk.service),
			zap.Reflect("shard", dv),
		)
		return addFn(dv)
	})

	for _, shardID := range dropShardIDs {
		if err := sk.storage.Remove(shardID); err != nil {
			return err
		}
	}

	for shardID, dv := range updateDbValues {
		dv.Disp = true
		if err := sk.storage.Put(shardID, dv); err != nil {
			return err
		}
	}

	// 整体sync一遍，才进入运行时根据Disp属性选择同步状态
	if !sk.initialized {
		sk.initialized = true
	}
	return nil
}

func (sk *shardKeeper) Close() {
	if sk.stopper != nil {
		sk.stopper.Close()
	}

	if sk.rbTrigger != nil {
		sk.rbTrigger.Close()
	}

	if sk.storage != nil {
		sk.storage.Close()
	}

	sk.lg.Info(
		"active closed",
		zap.String("service", sk.service),
	)
}
