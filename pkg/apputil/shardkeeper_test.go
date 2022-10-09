package apputil

import (
	"reflect"
	"testing"

	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	defaultTestPlaceHolder = "defaultTestPlaceHolder"
)

var (
	_ ShardInterface = new(MockedShardInterface)
)

func TestShardKeeper(t *testing.T) {
	suite.Run(t, new(ShardKeeperTestSuite))
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

	mockedStorage := new(storage.MockedStorage)
	mockedStorage.On("DropByLease", false, sl.Lease.ID).Return(nil)
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

	mockedStorage := new(storage.MockedStorage)
	mockedStorage.On("DropByLease", false, sl.Lease.ID).Return(nil)
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

	mockedStorage := new(storage.MockedStorage)
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

	mockedStorage := new(storage.MockedStorage)
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

	mockedStorage := new(storage.MockedStorage)
	mockedStorage.On("MigrateLease", suite.shardKeeper.bridgeLease.ID, sl.ID).Return(nil)
	mockedStorage.On("DropByLease", true, sl.ID).Return(nil)
	suite.shardKeeper.storage = mockedStorage

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("Put",
		mock.Anything,
		etcdutil.LeaseSessionPath(suite.shardKeeper.service, suite.shardKeeper.containerId),
		mock.Anything,
		mock.Anything).Return(&clientv3.PutResponse{}, nil)
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

	mockedStorage := new(storage.MockedStorage)
	mockedStorage.On("Add", fakeSpec).Return(nil)
	suite.shardKeeper.storage = mockedStorage
	err := suite.shardKeeper.Add(fakeShardId, fakeSpec)
	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ShardKeeperTestSuite) TestDrop_notExist() {
	fakeShardId := defaultTestPlaceHolder

	mockedStorage := new(storage.MockedStorage)
	mockedStorage.On("Drop", []string{fakeShardId}).Return(nil)
	suite.shardKeeper.storage = mockedStorage
	err := suite.shardKeeper.Drop(fakeShardId)
	mockedStorage.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}
