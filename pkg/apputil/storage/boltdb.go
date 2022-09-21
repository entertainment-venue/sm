package storage

import (
	"encoding/json"
	"math"
	"path/filepath"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var _ Storage = &boltdb{}

type boltdb struct {
	service string
	lg      *zap.Logger

	db *bolt.DB
}

func NewBoltdb(dir string, service string, lg *zap.Logger) (*boltdb, error) {
	db, err := bolt.Open(filepath.Join(dir, "shard.db"), 0600, nil)
	if err != nil {
		return nil, err
	}
	if err := db.Update(
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(service))
			return errors.Wrap(err, "")
		},
	); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return &boltdb{service: service, db: db, lg: lg}, nil
}

func (bt *boltdb) Close() error {
	if bt.db != nil {
		return bt.db.Close()
	}
	return nil
}

func (bt *boltdb) Add(shard *ShardSpec) error {
	value := &ShardKeeperDbValue{
		Spec: shard,
		Disp: false,
		Drop: false,
	}

	return bt.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))

		k := []byte(shard.Id)
		v := b.Get(k)
		if v != nil {
			// id已经存在，不需要写入boltdb
			bt.lg.Info(
				"shard already exist",
				zap.String("service", bt.service),
				zap.String("id", shard.Id),
			)
			return nil
		}

		bt.lg.Info(
			"shard added to boltdb",
			zap.String("service", bt.service),
			zap.String("id", shard.Id),
		)
		return errors.Wrap(b.Put(k, []byte(value.String())), "")
	})
}

func (bt *boltdb) Drop(ids []string) error {
	if len(ids) == 0 {
		bt.lg.Warn(
			"empty ids",
			zap.String("service", bt.service),
		)
		return nil
	}
	shardIDMap := make(map[string]struct{})
	for _, id := range ids {
		shardIDMap[id] = struct{}{}
	}

	return bt.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))
		for shardID := range shardIDMap {
			key := []byte(shardID)
			raw := b.Get(key)
			if raw == nil {
				bt.lg.Warn(
					"shard not exist when try to drop",
					zap.String("service", bt.service),
					zap.String("shard-id", shardID),
				)
				continue
			}

			var dv ShardKeeperDbValue
			if err := json.Unmarshal(raw, &dv); err != nil {
				// 不会发生这种情况，除非测试case
				if err := b.Delete(key); err != nil {
					return errors.Wrap(err, "")
				}
				bt.lg.Warn(
					"shard deleted directly because format error",
					zap.String("service", bt.service),
					zap.String("shard-id", shardID),
				)
				continue
			}
			dv.Disp = false
			dv.Drop = true
			if err := b.Put([]byte(shardID), []byte(dv.String())); err != nil {
				return errors.Wrap(err, "")
			}

			bt.lg.Info(
				"drop shard success",
				zap.String("shard-id", shardID),
				zap.Int64("cur-lease", int64(dv.Spec.Lease.ID)),
			)
		}
		return nil
	})
}

func (bt *boltdb) ForEach(visitor func(k []byte, v []byte) error) error {
	return bt.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bt.service))
			return b.ForEach(visitor)
		},
	)
}

func (bt *boltdb) MigrateLease(from, to clientv3.LeaseID) error {
	return bt.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))
		return b.ForEach(func(k, v []byte) error {
			var dv ShardKeeperDbValue
			if err := json.Unmarshal(v, &dv); err != nil {
				return errors.Wrap(err, "")
			}

			// 不需要做移动，逻辑幂等的一部分
			if dv.Spec.Lease.ID == to {
				bt.lg.Info(
					"shard lease already be to",
					zap.String("service", bt.service),
					zap.String("shard-id", dv.Spec.Id),
					zap.Int64("to", int64(to)),
				)
				return nil
			}

			if dv.Spec.Lease.ID == from {
				bt.lg.Info(
					"migrate lease",
					zap.String("service", bt.service),
					zap.String("shard-id", dv.Spec.Id),
					zap.Reflect("from", int64(from)),
					zap.Int64("to", int64(to)),
				)
				dv.Spec.Lease = &Lease{ID: to, Expire: math.MaxInt64 - 30}
			} else {
				bt.lg.Warn(
					"drop lease",
					zap.String("service", bt.service),
					zap.String("shard-id", dv.Spec.Id),
					zap.Reflect("lease-id", int64(dv.Spec.Lease.ID)),
					zap.Reflect("from", int64(from)),
					zap.Int64("to", int64(to)),
				)
				// 异步删除，下发drop指令到app，通过sync goroutine
				dv.Disp = false
				dv.Drop = true
			}
			return b.Put(k, []byte(dv.String()))
		})
	})
}

func (bt *boltdb) DropByLease(leaseID clientv3.LeaseID, exclude bool) error {
	if exclude && leaseID == clientv3.NoLease {
		panic("unexpected error")
	}
	return bt.db.Update(func(tx *bolt.Tx) error {
		var dropCnt int
		b := tx.Bucket([]byte(bt.service))
		err := b.ForEach(func(k, v []byte) error {
			var dv ShardKeeperDbValue
			if err := json.Unmarshal(v, &dv); err != nil {
				return errors.Wrap(err, "")
			}

			var needDrop bool
			if exclude {
				//  除leaseID都删除
				if leaseID == clientv3.NoLease || leaseID != dv.Spec.Lease.ID {
					needDrop = true
				}
			} else {
				// 只有leaseID需要被删除
				if leaseID == dv.Spec.Lease.ID {
					needDrop = true
				}
			}

			if needDrop {
				dv.Disp = false
				dv.Drop = true
				bt.lg.Info(
					"drop shard",
					zap.String("service", bt.service),
					zap.String("shard-id", dv.Spec.Id),
					zap.Bool("exclude", exclude),
					zap.Int64("cur-lease", int64(dv.Spec.Lease.ID)),
					zap.Int64("lease-id", int64(leaseID)),
				)
				if err := b.Put(k, []byte(dv.String())); err != nil {
					return errors.Wrap(err, "")
				}
				dropCnt++
			}

			return nil
		})
		if err != nil {
			return errors.Wrap(err, "")
		}
		bt.lg.Info(
			"drop by lease",
			zap.String("service", bt.service),
			zap.Int("drop-cnt", dropCnt),
		)
		return nil
	})
}

func (bt *boltdb) CompleteDispatch(id string, del bool) error {
	return bt.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))
		if del {
			return errors.Wrap(b.Delete([]byte(id)), "")
		}
		v := b.Get([]byte(id))
		if v == nil {
			return errors.Errorf("not exist %s del %t", id, del)
		}
		var dv ShardKeeperDbValue
		if err := json.Unmarshal(v, &dv); err != nil {
			return errors.Wrap(err, "")
		}
		dv.Disp = true
		return errors.Wrap(b.Put([]byte(id), []byte(dv.String())), "")
	})
}

func (bt *boltdb) Reset() error {
	return bt.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))
		return b.ForEach(func(k, v []byte) error {
			var dv ShardKeeperDbValue
			if err := json.Unmarshal(v, &dv); err != nil {
				return errors.Wrap(err, "")
			}
			dv.Disp = false
			return errors.Wrap(b.Put(k, []byte(dv.String())), "")
		})
	})
}

func (bt *boltdb) Update(k, v []byte) error {
	return bt.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))
		return b.Put(k, v)
	})
}

func (bt *boltdb) Delete(k []byte) error {
	return bt.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))
		return b.Delete(k)
	})
}

func (bt *boltdb) Get(k []byte) ([]byte, error) {
	var r []byte
	err := bt.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bt.service))
		r = b.Get(k)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return r, nil
}

func (bt *boltdb) Clear() error {
	return bt.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(bt.service))
	})
}
