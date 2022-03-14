package apputil

import (
	"fmt"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func testNewDb(bucket string) (*bolt.DB, error) {
	db, err := bolt.Open("debug.db", 0600, nil)
	if err != nil {
		return nil, err
	}
	if err := db.Update(
		func(tx *bolt.Tx) error {
			_ = tx.DeleteBucket([]byte(bucket))
			_, _ = tx.CreateBucket([]byte(bucket))
			return nil
		},
	); err != nil {
		return nil, err
	}
	return db, nil
}

func Test_shardKeeper_Add(t *testing.T) {
	sk := shardKeeper{service: "test"}
	sk.db, _ = testNewDb(sk.service)
	if err := sk.Add("foo", &ShardSpec{Service: "bar"}); err != nil {
		t.Error(err)
		t.SkipNow()
	}
}

func Test_shardKeeper_Drop(t *testing.T) {
	sk := shardKeeper{service: "test"}
	sk.db, _ = testNewDb(sk.service)

	key := "foo"

	sk.db.Update(
		func(tx *bolt.Tx) error {
			dv := shardKeeperDbValue{
				Spec: &ShardSpec{Service: "bar"},
			}
			b := tx.Bucket([]byte(sk.service))
			return b.Put([]byte(key), []byte(dv.String()))
		},
	)

	if err := sk.Drop(key); err != nil {
		t.Error(err)
		t.SkipNow()
	}
}

func Test_shardKeeper_forEach(t *testing.T) {
	sk := shardKeeper{service: "test"}
	sk.db, _ = testNewDb(sk.service)

	sk.db.Update(
		func(tx *bolt.Tx) error {
			dv := shardKeeperDbValue{
				Spec: &ShardSpec{Service: "bar"},
			}
			b := tx.Bucket([]byte(sk.service))

			b.Put([]byte("foo"), []byte(dv.String()))
			b.Put([]byte("bar"), []byte(dv.String()))
			return nil
		},
	)

	if err := sk.forEachRead(func(k, v []byte) error {
		fmt.Println(string(k))
		return nil
	}); err != nil {
		t.Error(err)
		t.SkipNow()
	}
}

func Test_shardKeeper_sync(t *testing.T) {
	sk := shardKeeper{service: "test"}
	sk.db, _ = testNewDb(sk.service)

	sk.db.Update(
		func(tx *bolt.Tx) error {
			dv := shardKeeperDbValue{
				Spec: &ShardSpec{Service: "bar"},
				Disp: true,
				Drop: true,
			}
			b := tx.Bucket([]byte(sk.service))

			b.Put([]byte("foo"), []byte(dv.String()))
			b.Put([]byte("bar"), []byte(dv.String()))
			return nil
		},
	)

	if err := sk.sync(); err != nil {
		t.Error(err)
		t.SkipNow()
	}
}
