package storage

import (
	"context"
	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var _ Storage = new(etcddb)

type etcddb struct {
	service string
	lg      *zap.Logger

	client etcdutil.EtcdWrapper
}

func NewEtcddb(service string, client etcdutil.EtcdWrapper, lg *zap.Logger) (*etcddb, error) {
	return &etcddb{
		service: service,
		lg:      lg,

		client: client,
	}, nil
}

func (e *etcddb) Close() error {
	if e == nil || e.client == nil {
		return nil
	}

	return e.client.Close()
}

func (e *etcddb) Add(shard *ShardSpec) error {
	value := &ShardKeeperDbValue{
		Spec: shard,
		Disp: false,
		Drop: false,
	}
	shardPath := apputil.ShardPath(e.service, shard.Id)
	if err := e.client.CreateAndGet(context.TODO(), []string{shardPath}, []string{value.String()}, clientv3.NoLease); err != nil {
		e.lg.Error(
			"CreateAndGet error",
			zap.String("service", e.service),
			zap.String("shard-path", shardPath),
			zap.Error(err),
		)
		return err
	}
	e.lg.Info(
		"shard added to etcddb",
		zap.String("service", e.service),
		zap.String("id", shard.Id),
	)
	return nil
}

func (e *etcddb) Drop(ids []string) error {
	if len(ids) == 0 {
		e.lg.Warn(
			"empty ids",
			zap.String("service", e.service),
		)
		return nil
	}

	var keys []string
	for _, id := range ids {
		keys = append(keys, apputil.ShardPath(e.service, id))
	}

	if err := e.client.DelKVs(context.TODO(), keys); err != nil {
		e.lg.Info(
			"drop shard error",
			zap.String("service", e.service),
			zap.Strings("shard-ids", ids),
			zap.Error(err),
		)
		return err
	}
	e.lg.Info(
		"drop shard success",
		zap.String("service", e.service),
		zap.Strings("shard-ids", ids),
	)
	return nil
}

func (e *etcddb) ForEach(visitor func(k []byte, v []byte) error) error {
	shardDir := apputil.ShardDir(e.service)
	kvs, err := e.client.GetKVs(context.TODO(), shardDir)
	if err != nil {
		return err
	}
	for k, v := range kvs {
		if err := visitor([]byte(k), []byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func (e *etcddb) MigrateLease(from, to clientv3.LeaseID) error {
	panic("implement me")
}

func (e *etcddb) DropByLease(leaseID clientv3.LeaseID, exclude bool) error {
	panic("implement me")
}

func (e *etcddb) CompleteDispatch(id string, del bool) error {
	panic("implement me")
}

func (e *etcddb) Reset() error {
	panic("implement me")
}

func (e *etcddb) Update(k, v []byte) error {
	panic("implement me")
}

func (e *etcddb) Delete(k []byte) error {
	panic("implement me")
}

func (e *etcddb) Get(k []byte) ([]byte, error) {
	panic("implement me")
}

func (e *etcddb) Clear() error {
	panic("implement me")
}
