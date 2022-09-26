package storage

import (
	"encoding/json"
	"path/filepath"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var _ Storage = new(boltdb)

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

func (db *boltdb) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *boltdb) Add(shard *ShardSpec) error {
	value := &ShardKeeperDbValue{
		Spec: shard,
		Disp: false,
		Drop: false,
	}

	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))

		k := []byte(shard.Id)
		v := b.Get(k)
		if v != nil {
			// id已经存在，不需要写入boltdb
			db.lg.Info(
				"shard already exist",
				zap.String("service", db.service),
				zap.String("id", shard.Id),
			)
			return nil
		}

		db.lg.Info(
			"shard added to boltdb",
			zap.String("service", db.service),
			zap.String("id", shard.Id),
		)
		return errors.Wrap(b.Put(k, []byte(value.String())), "")
	})
}

func (db *boltdb) Drop(ids []string) error {
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

	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
		for shardID := range shardIDMap {
			key := []byte(shardID)
			raw := b.Get(key)
			if raw == nil {
				db.lg.Warn(
					"shard not exist when try to drop",
					zap.String("service", db.service),
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
				db.lg.Warn(
					"shard deleted directly because format error",
					zap.String("service", db.service),
					zap.String("shard-id", shardID),
				)
				continue
			}
			dv.Disp = false
			dv.Drop = true
			if err := b.Put([]byte(shardID), []byte(dv.String())); err != nil {
				return errors.Wrap(err, "")
			}

			db.lg.Info(
				"drop shard success",
				zap.String("shard-id", shardID),
				zap.Int64("cur-lease", int64(dv.Spec.Lease.ID)),
			)
		}
		return nil
	})
}

func (db *boltdb) ForEach(visitor func(shardID string, dv *ShardKeeperDbValue) error) error {
	var dropShardIDs []string
	err := db.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(db.service))
			return b.ForEach(func(k, v []byte) error {
				var dv ShardKeeperDbValue
				if err := json.Unmarshal(v, &dv); err != nil {
					dropShardIDs = append(dropShardIDs, dv.Spec.Id)
					return err
				}
				return visitor(dv.Spec.Id, &dv)
			})
		},
	)
	if len(dropShardIDs) > 0 {
		if err := db.Drop(dropShardIDs); err != nil {
			db.lg.Error(
				"Drop error",
				zap.String("service", db.service),
				zap.Strings("shard-id", dropShardIDs),
			)
		}
	}
	return err
}

func (db *boltdb) MigrateLease(from, to clientv3.LeaseID) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
		return b.ForEach(func(k, v []byte) error {
			var dv ShardKeeperDbValue
			if err := json.Unmarshal(v, &dv); err != nil {
				return errors.Wrap(err, "")
			}

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
			return b.Put(k, []byte(dv.String()))
		})
	})
}

func (db *boltdb) DropByLease(exclude bool, leaseID clientv3.LeaseID) error {
	if exclude && leaseID == clientv3.NoLease {
		panic("unexpected error")
	}
	return db.db.Update(func(tx *bolt.Tx) error {
		var dropCnt int
		b := tx.Bucket([]byte(db.service))
		if err := b.ForEach(func(k, v []byte) error {
			var dv ShardKeeperDbValue
			if err := json.Unmarshal(v, &dv); err != nil {
				return errors.Wrap(err, "")
			}

			if dv.NeedDrop(exclude, leaseID) {
				dv.Disp = false
				dv.Drop = true
				db.lg.Info(
					"drop shard",
					zap.String("service", db.service),
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
		}); err != nil {
			return errors.Wrap(err, "")
		}
		db.lg.Info(
			"drop by lease",
			zap.String("service", db.service),
			zap.Int("drop-cnt", dropCnt),
		)
		return nil
	})
}

func (db *boltdb) Reset() error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
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

func (db *boltdb) Put(shardID string, dv *ShardKeeperDbValue) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
		v := b.Get([]byte(shardID))
		if v == nil {
			return errors.Errorf("not exist %s", shardID)
		}
		return errors.Wrap(b.Put([]byte(shardID), []byte(dv.String())), "")
	})
}

func (db *boltdb) Remove(shardID string) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
		return errors.Wrap(b.Delete([]byte(shardID)), "")
	})
}

func (db *boltdb) Update(k, v []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
		return b.Put(k, v)
	})
}

func (db *boltdb) Delete(k []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
		return b.Delete(k)
	})
}

func (db *boltdb) Get(k []byte) ([]byte, error) {
	var r []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(db.service))
		r = b.Get(k)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return r, nil
}

func (db *boltdb) Clear() error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(db.service))
	})
}
