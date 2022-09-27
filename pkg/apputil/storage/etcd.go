package storage

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var _ Storage = new(etcddb)

type etcddb struct {
	service     string
	containerId string
	lg          *zap.Logger

	mu struct {
		sync.Mutex
		kvs map[string]*ShardKeeperDbValue
	}

	// 单纯通过etcd做，要实现shard额软删除，需要标记etcd的key中的value，然后有goroutine感知，并同步到内存中的snapshot，
	// shardkeeper感知的是这部分数据的变化。
	client etcdutil.EtcdWrapper
}

func NewEtcddb(service string, containerId string, client etcdutil.EtcdWrapper, lg *zap.Logger) (*etcddb, error) {
	db := &etcddb{
		service:     service,
		containerId: containerId,
		lg:          lg,

		client: client,
	}
	db.mu.kvs = make(map[string]*ShardKeeperDbValue)
	return db, nil
}

func (db *etcddb) Close() error {
	if db == nil || db.client == nil {
		return nil
	}

	return db.client.Close()
}

func (db *etcddb) Add(shard *ShardSpec) error {
	value := &ShardKeeperDbValue{
		Spec: shard,
		Disp: false,
		Drop: false,
	}
	shardPath := etcdutil.ShardPath(db.service, db.containerId, shard.Id)

	// 下面这段是辅助追查问题
	gresp, err := db.client.GetKV(context.TODO(), shardPath, nil)
	if err != nil {
		return err
	}
	if gresp.Count > 0 {
		db.lg.Warn(
			"etcd already has value",
			zap.String("service", db.service),
			zap.String("shard-path", shardPath),
			zap.ByteString("value", gresp.Kvs[0].Value),
		)
	}

	if err := db.client.UpdateKV(context.TODO(), shardPath, value.String()); err != nil {
		db.lg.Error(
			"UpdateKV err",
			zap.String("service", db.service),
			zap.String("shard-path", shardPath),
			zap.Error(err),
		)
		return err
	}
	db.lg.Info(
		"add shard success",
		zap.String("service", db.service),
		zap.String("shard-path", shardPath),
	)

	// 先写etcd，对于应用程序没有干扰，然后写memory，保证一致，这块写失败不太可能
	db.mu.Lock()
	defer db.mu.Unlock()
	db.mu.kvs[shard.Id] = value

	return nil
}

func (db *etcddb) Drop(ids []string) error {
	if len(ids) == 0 {
		db.lg.Warn(
			"empty ids",
			zap.String("service", db.service),
		)
		return nil
	}

	shardIDMap := make(map[string]struct{})
	for _, id := range ids {
		shardIDMap[id] = struct{}{}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 内存中drop掉，sync方法同步给应用和etcd，保证etcd同步之后，才可以给到应用
	for shardID, dv := range db.mu.kvs {
		if _, ok := shardIDMap[shardID]; ok {
			dv.Disp = false
			dv.Drop = true

			db.lg.Info(
				"drop shard success",
				zap.String("shard-id", shardID),
			)
		}
	}
	return nil
}

func (db *etcddb) ForEach(visitor func(shardID string, dv *ShardKeeperDbValue) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, dv := range db.mu.kvs {
		if err := visitor(dv.Spec.Id, dv); err != nil {
			return err
		}
	}
	return nil
}

func (db *etcddb) MigrateLease(from, to clientv3.LeaseID) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// assert
	if to == clientv3.NoLease {
		panic("unexpected error")
	}

	for _, dv := range db.mu.kvs {
		dv.SoftMigrate(from, to)
		db.lg.Info(
			"SoftMigrate success",
			zap.String("service", db.service),
			zap.String("shard-id", dv.Spec.Id),
			zap.Reflect("from", int64(from)),
			zap.Int64("to", int64(to)),
			zap.Bool("disp", dv.Disp),
			zap.Bool("drop", dv.Drop),
		)
	}

	return nil
}

func (db *etcddb) DropByLease(exclude bool, leaseID clientv3.LeaseID) error {
	if exclude && leaseID == clientv3.NoLease {
		panic("unexpected error")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	var dropCnt int
	for _, dv := range db.mu.kvs {
		if dv.NeedDrop(exclude, leaseID) {
			dv.Disp = false
			dv.Drop = true
			db.lg.Info(
				"DropByLease success",
				zap.String("service", db.service),
				zap.String("shard-id", dv.Spec.Id),
				zap.Bool("exclude", exclude),
				zap.Int64("cur-lease", int64(dv.Spec.Lease.ID)),
				zap.Int64("lease-id", int64(leaseID)),
			)
			dropCnt++
		}
	}
	db.lg.Info(
		"drop by lease",
		zap.String("service", db.service),
		zap.Int("drop-cnt", dropCnt),
	)
	return nil
}

func (db *etcddb) Reset() error {
	// assert
	if len(db.mu.kvs) > 0 {
		panic("unexpected error")
	}

	// etcd场景，通过Reset重新加载etcd中的分片
	dir := etcdutil.ShardDir(db.service, db.containerId)
	kvs, err := db.client.GetKVs(context.TODO(), dir)
	if err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for shardID, v := range kvs {
		var dv ShardKeeperDbValue
		if err := json.Unmarshal([]byte(v), &dv); err != nil {
			return errors.Wrap(err, "")
		}
		dv.Disp = false
		db.mu.kvs[shardID] = &dv
	}
	return nil
}

// Put 写入etcd后才能，标记缓存
func (db *etcddb) Put(shardID string, dv *ShardKeeperDbValue) error {
	if err := db.client.UpdateKV(context.TODO(), etcdutil.ShardPath(db.service, db.containerId, shardID), dv.String()); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.mu.kvs[shardID] = dv
	return nil
}

// Remove 移除etcd后才能，删除缓存
func (db *etcddb) Remove(shardID string) error {
	if err := db.client.DelKV(context.TODO(), etcdutil.ShardPath(db.service, db.containerId, shardID)); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.mu.kvs, shardID)
	return nil
}

func (db *etcddb) Update(k, v []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var dv ShardKeeperDbValue
	if err := json.Unmarshal(v, &dv); err != nil {
		return err
	}
	db.mu.kvs[string(k)] = &dv
	return nil
}

func (db *etcddb) Delete(k []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	delete(db.mu.kvs, string(k))
	return nil
}

func (db *etcddb) Get(k []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	v := db.mu.kvs[string(k)]
	if v == nil {
		return nil, nil
	}
	b, _ := json.Marshal(v)
	return b, nil
}

func (db *etcddb) Clear() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.mu.kvs = make(map[string]*ShardKeeperDbValue)
	return nil
}
