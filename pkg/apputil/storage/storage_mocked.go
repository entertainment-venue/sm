package storage

import (
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ Storage = new(MockedStorage)

type MockedStorage struct {
	mock.Mock
}

func (m *MockedStorage) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockedStorage) Add(shard *ShardSpec) error {
	args := m.Called(shard)
	return args.Error(0)
}

func (m *MockedStorage) Drop(ids []string) error {
	args := m.Called(ids)
	return args.Error(0)
}

func (m *MockedStorage) ForEach(visitor func(shardID string, dv *ShardKeeperDbValue) error) error {
	panic("implement me")
}

func (m *MockedStorage) MigrateLease(from, to clientv3.LeaseID) error {
	args := m.Called(from, to)
	return args.Error(0)
}

func (m *MockedStorage) DropByLease(exclude bool, leaseID clientv3.LeaseID) error {
	args := m.Called(exclude, leaseID)
	return args.Error(0)
}

func (m *MockedStorage) Reset() error {
	panic("implement me")
}

func (m *MockedStorage) Put(shardID string, dv *ShardKeeperDbValue) error {
	args := m.Called(shardID, dv)
	return args.Error(0)
}

func (m *MockedStorage) Remove(shardID string) error {
	panic("implement me")
}

func (m *MockedStorage) Update(k, v []byte) error {
	args := m.Called(k, v)
	return args.Error(0)
}

func (m *MockedStorage) Delete(k []byte) error {
	panic("implement me")
}

func (m *MockedStorage) Get(k []byte) ([]byte, error) {
	panic("implement me")
}

func (m *MockedStorage) Clear() error {
	args := m.Called()
	return args.Error(0)
}
