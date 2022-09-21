package apputil

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/zd3tl/evtrigger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	defaultTestPlaceHolder = "defaultTestPlaceHolder"
)

var (
	_ evtrigger.Trigger    = new(MockedTrigger)
	_ ShardInterface       = new(MockedShardInterface)
	_ storage.Storage      = new(MockedStorage)
	_ etcdutil.EtcdWrapper = new(MockedEtcdWrapper)
)

func TestShardKeeper(t *testing.T) {
	suite.Run(t, new(ShardKeeperTestSuite))
}

type MockedEtcdWrapper struct {
	mock.Mock
}

func (m MockedEtcdWrapper) Close() error {
	panic("implement me")
}

func (m MockedEtcdWrapper) GetClient() *etcdutil.EtcdClient {
	panic("implement me")
}

func (m MockedEtcdWrapper) GetKV(_ context.Context, node string, opts []clientv3.OpOption) (*clientv3.GetResponse, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) GetKVs(ctx context.Context, prefix string) (map[string]string, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) UpdateKV(ctx context.Context, key string, value string) error {
	panic("implement me")
}

func (m MockedEtcdWrapper) DelKV(ctx context.Context, prefix string) error {
	panic("implement me")
}

func (m MockedEtcdWrapper) DelKVs(ctx context.Context, prefixes []string) error {
	panic("implement me")
}

func (m MockedEtcdWrapper) CreateAndGet(ctx context.Context, nodes []string, values []string, leaseID clientv3.LeaseID) error {
	args := m.Called(ctx, nodes, values, leaseID)
	return args.Error(0)
}

func (m MockedEtcdWrapper) CompareAndSwap(_ context.Context, node string, curValue string, newValue string, leaseID clientv3.LeaseID) (string, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) Inc(_ context.Context, pfx string) (string, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) NewSession(ctx context.Context, client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) Ctx() context.Context {
	panic("implement me")
}

func (m MockedEtcdWrapper) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	panic("implement me")
}

func (m MockedEtcdWrapper) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	panic("implement me")
}

type MockedTrigger struct {
	mock.Mock
}

func (m *MockedTrigger) Register(key string, callback evtrigger.TriggerCallback) error {
	panic("implement me")
}

func (m *MockedTrigger) Put(event *evtrigger.TriggerEvent) error {
	args := m.Called(event)
	return args.Error(0)
}

func (m *MockedTrigger) ForEach(visitor func(it interface{}) error) error {
	panic("implement me")
}

func (m *MockedTrigger) Close() {
	panic("implement me")
}

type MockedShardInterface struct {
	mock.Mock
}

func (m *MockedShardInterface) Add(id string, spec *storage.ShardSpec) error {
	args := m.Called(id, spec)
	return args.Error(0)
}

func (m *MockedShardInterface) Drop(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

type MockedStorage struct {
	mock.Mock
}

func (m MockedStorage) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m MockedStorage) Add(shard *storage.ShardSpec) error {
	args := m.Called(shard)
	return args.Error(0)
}

func (m MockedStorage) Drop(ids []string) error {
	args := m.Called(ids)
	return args.Error(0)
}

func (m MockedStorage) ForEach(visitor func(k []byte, v []byte) error) error {
	panic("implement me")
}

func (m MockedStorage) MigrateLease(from, to clientv3.LeaseID) error {
	args := m.Called(from, to)
	return args.Error(0)
}

func (m MockedStorage) DropByLease(leaseID clientv3.LeaseID, exclude bool) error {
	args := m.Called(leaseID, exclude)
	return args.Error(0)
}

func (m MockedStorage) CompleteDispatch(id string, del bool) error {
	panic("implement me")
}

func (m MockedStorage) Reset() error {
	panic("implement me")
}

func (m MockedStorage) Update(k, v []byte) error {
	panic("implement me")
}

func (m MockedStorage) Delete(k []byte) error {
	panic("implement me")
}

func (m MockedStorage) Get(k []byte) ([]byte, error) {
	panic("implement me")
}

func (m MockedStorage) Clear() error {
	args := m.Called()
	return args.Error(0)
}

type ShardKeeperTestSuite struct {
	suite.Suite

	shardKeeper  *shardKeeper
	shardDbValue *storage.ShardKeeperDbValue
}

func (suite *ShardKeeperTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()
	service := "foo"
	defaultLease := storage.Lease{ID: 100, Expire: 100}

	suite.shardKeeper = &shardKeeper{
		service:     service,
		lg:          lg,
		bridgeLease: storage.NoLease,
		guardLease:  &defaultLease,
	}

	suite.shardDbValue = &storage.ShardKeeperDbValue{
		Spec: &storage.ShardSpec{Id: "bar", Lease: &defaultLease},
		Disp: true,
		Drop: false,
	}
}

func (suite *ShardKeeperTestSuite) TestHandleRbEvent_typeError() {
	err := suite.shardKeeper.handleRbEvent("", "")
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestHandleRbEvent_parseShardLeaseError() {
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Value: []byte("")},
	}
	err := suite.shardKeeper.handleRbEvent("", &ev)
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestParseLease_leaseError() {
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Value: []byte("")},
	}
	_, err := suite.shardKeeper.parseShardLease(&ev)
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestParseLease_leaseOk() {
	sl := ShardLease{}
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv:   &mvccpb.KeyValue{Value: []byte(sl.String())},
	}
	actual, err := suite.shardKeeper.parseShardLease(&ev)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), reflect.DeepEqual(*actual, sl))
}

func (suite *ShardKeeperTestSuite) TestHandleSessionKeyEvent_create() {
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value:          []byte(""),
			CreateRevision: 1,
			ModRevision:    1,
		},
	}
	err := suite.shardKeeper.handleSessionKeyEvent(&ev)
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestHandleSessionKeyEvent_delete() {
	sl := ShardLease{}
	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		PrevKv: &mvccpb.KeyValue{
			Value: []byte(sl.String()),
		},
	}

	mockedStorage := new(MockedStorage)
	mockedStorage.On("DropByLease", sl.Lease.ID, false).Return(nil)
	suite.shardKeeper.storage = mockedStorage

	err := suite.shardKeeper.handleSessionKeyEvent(&ev)
	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireBridgeLease_modify() {
	// 	acquireBridgeLease do not accept modify event, cause bridge no update logic
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value:          []byte(""),
			CreateRevision: 1,
			ModRevision:    2,
		},
	}
	err := suite.shardKeeper.acquireBridgeLease(&ev, nil)
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireBridgeLease_delete() {
	// delete bridge event, require no shard has the bridge now
	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Value:          []byte(""),
			CreateRevision: 1,
			ModRevision:    2,
		},
	}

	sl := ShardLease{}

	mockedStorage := new(MockedStorage)
	mockedStorage.On("DropByLease", sl.Lease.ID, false).Return(nil)
	suite.shardKeeper.storage = mockedStorage

	err := suite.shardKeeper.acquireBridgeLease(&ev, &sl)
	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireBridgeLease_guardLeaseError() {
	// create event, need drop some shard, then migrate current guard to bridge
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: []byte(""),
		},
	}

	sl := ShardLease{
		// not equal current global value
		GuardLeaseID: suite.shardDbValue.Spec.Lease.ID + 1,
		Assignment: &Assignment{
			Drops: []string{},
		},
	}

	mockedStorage := new(MockedStorage)
	mockedStorage.On("Drop", mock.Anything).Return(nil)
	suite.shardKeeper.storage = mockedStorage

	err := suite.shardKeeper.acquireBridgeLease(&ev, &sl)
	mockedStorage.AssertExpectations(suite.T())
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireBridgeLease_ok() {
	// create event, need drop some shard, then migrate current guard to bridge
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: []byte(""),
		},
	}

	sl := ShardLease{
		// not equal current global value
		GuardLeaseID: suite.shardDbValue.Spec.Lease.ID,
		Assignment: &Assignment{
			Drops: []string{},
		},
	}

	mockedStorage := new(MockedStorage)
	mockedStorage.On("Drop", mock.Anything).Return(nil)
	mockedStorage.On("MigrateLease", sl.GuardLeaseID, sl.ID).Return(nil)
	suite.shardKeeper.storage = mockedStorage

	err := suite.shardKeeper.acquireBridgeLease(&ev, &sl)
	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireGuardLease_create() {
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: []byte(""),
		},
	}

	err := suite.shardKeeper.acquireGuardLease(&ev, nil)
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireGuardLease_bridgeLeaseNoLease() {
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: []byte(""),

			// update event
			CreateRevision: 1,
			ModRevision:    2,
		},
	}

	err := suite.shardKeeper.acquireGuardLease(&ev, nil)
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireGuardLease_bridgeLeaseError() {
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: []byte(""),

			// update event
			CreateRevision: 1,
			ModRevision:    2,
		},
	}

	suite.shardKeeper.bridgeLease = &storage.Lease{ID: 101}
	sl := ShardLease{
		// not equal current global value
		BridgeLeaseID: 102,
	}

	err := suite.shardKeeper.acquireGuardLease(&ev, &sl)
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAcquireGuardLease_ok() {
	ev := clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: []byte(""),

			// update event
			CreateRevision: 1,
			ModRevision:    2,
		},
	}

	suite.shardKeeper.bridgeLease = &storage.Lease{ID: 101}
	sl := ShardLease{
		// not equal current global value
		BridgeLeaseID: 101,
	}

	mockedStorage := new(MockedStorage)
	mockedStorage.On("MigrateLease", suite.shardKeeper.bridgeLease.ID, sl.ID).Return(nil)
	mockedStorage.On("DropByLease", sl.ID, true).Return(nil)
	suite.shardKeeper.storage = mockedStorage

	mockedEtcdWrapper := new(MockedEtcdWrapper)
	mockedEtcdWrapper.On("CreateAndGet", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.shardKeeper.client = mockedEtcdWrapper

	err := suite.shardKeeper.acquireGuardLease(&ev, &sl)

	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAdd_leaseNotEqual() {
	fakeShardId := mock.Anything
	fakeSpec := &storage.ShardSpec{
		Id:    fakeShardId,
		Lease: &storage.Lease{ID: 101},
	}
	err := suite.shardKeeper.Add(fakeShardId, fakeSpec)
	assert.NotNil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestAdd_addOnce() {
	fakeShardId := defaultTestPlaceHolder
	fakeSpec := &storage.ShardSpec{
		Id:    fakeShardId,
		Lease: &storage.Lease{ID: suite.shardDbValue.Spec.Lease.ID},
	}

	mockedStorage := new(MockedStorage)
	mockedStorage.On("Add", fakeSpec).Return(nil)
	suite.shardKeeper.storage = mockedStorage
	err := suite.shardKeeper.Add(fakeShardId, fakeSpec)
	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestDrop_notExist() {
	fakeShardId := defaultTestPlaceHolder

	mockedStorage := new(MockedStorage)
	mockedStorage.On("Drop", []string{fakeShardId}).Return(nil)
	suite.shardKeeper.storage = mockedStorage
	err := suite.shardKeeper.Drop(fakeShardId)
	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestSync_jsonUnmarshalError() {
	var err error
	err = suite.shardKeeper.storage.Update([]byte(suite.shardDbValue.Spec.Id), []byte("error format string"))
	assert.Nil(suite.T(), err)

	// 在存在非法value的情况下，没有错误返回，除非删除不掉
	err = suite.shardKeeper.sync()
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestSync_LeaseNotEqualGuardLease() {
	var err error
	suite.shardDbValue.Spec.Lease = &storage.Lease{
		ID: 12345678,
	}
	suite.shardKeeper.guardLease = &storage.Lease{
		ID: 87654321,
	}
	err = suite.shardKeeper.storage.Update([]byte(suite.shardDbValue.Spec.Id), []byte(suite.shardDbValue.String()))
	assert.Nil(suite.T(), err)

	mockedShardInterface := new(MockedShardInterface)
	mockedShardInterface.On("Drop", suite.shardDbValue.Spec.Id).Return(nil)
	suite.shardKeeper.shardImpl = mockedShardInterface
	err = suite.shardKeeper.sync()
	assert.Nil(suite.T(), err)
	time.Sleep(1 * time.Second)
	err = suite.shardKeeper.storage.ForEach(
		func(k, v []byte) error {
			if string(k) == suite.shardDbValue.Spec.Id {
				suite.T().Fatal("lease not equal guardLease,not delete value")
			}
			return nil
		})
	assert.Nil(suite.T(), err)
	suite.shardKeeper.storage.Clear()
	suite.shardKeeper.storage.Close()
}
