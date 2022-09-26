package etcdutil

import (
	"context"

	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var _ EtcdWrapper = new(MockedEtcdWrapper)

type MockedEtcdWrapper struct {
	mock.Mock
}

func (m *MockedEtcdWrapper) Close() error {
	panic("implement me")
}

func (m *MockedEtcdWrapper) GetClient() *EtcdClient {
	panic("implement me")
}

func (m *MockedEtcdWrapper) GetKV(ctx context.Context, node string, opts []clientv3.OpOption) (*clientv3.GetResponse, error) {
	args := m.Called(ctx, node, opts)
	return args.Get(0).(*clientv3.GetResponse), args.Error(1)
}

func (m *MockedEtcdWrapper) GetKVs(ctx context.Context, prefix string) (map[string]string, error) {
	args := m.Called(ctx, prefix)
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockedEtcdWrapper) UpdateKV(ctx context.Context, key string, value string) error {
	args := m.Called(ctx, key, value)
	return args.Error(0)
}

func (m *MockedEtcdWrapper) DelKV(ctx context.Context, prefix string) error {
	panic("implement me")
}

func (m *MockedEtcdWrapper) DelKVs(ctx context.Context, prefixes []string) error {
	panic("implement me")
}

func (m *MockedEtcdWrapper) CreateAndGet(ctx context.Context, nodes []string, values []string, leaseID clientv3.LeaseID) error {
	args := m.Called(ctx, nodes, values, leaseID)
	return args.Error(0)
}

func (m *MockedEtcdWrapper) CompareAndSwap(_ context.Context, node string, curValue string, newValue string, leaseID clientv3.LeaseID) (string, error) {
	panic("implement me")
}

func (m *MockedEtcdWrapper) Inc(_ context.Context, pfx string) (string, error) {
	panic("implement me")
}

func (m *MockedEtcdWrapper) NewSession(ctx context.Context, client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
	panic("implement me")
}

func (m *MockedEtcdWrapper) Ctx() context.Context {
	panic("implement me")
}

func (m *MockedEtcdWrapper) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	panic("implement me")
}

func (m *MockedEtcdWrapper) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	args := m.Called(ctx, key, val, opts)
	return args.Get(0).(*clientv3.PutResponse), args.Error(1)
}

func (m *MockedEtcdWrapper) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	panic("implement me")
}

func (m *MockedEtcdWrapper) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	panic("implement me")
}
