package apputil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/zd3tl/evtrigger"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	_ evtrigger.Trigger = new(MockedTrigger)
	_ ShardInterface    = new(MockedShardInterface)
)

func TestShardKeeper(t *testing.T) {
	suite.Run(t, new(ShardKeeperTestSuite))
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

func (m *MockedShardInterface) Add(id string, spec *ShardSpec) error {
	args := m.Called(id, spec)
	return args.Error(0)
}

func (m *MockedShardInterface) Drop(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

type ShardKeeperTestSuite struct {
	suite.Suite

	shardKeeper *shardKeeper
	curShard    *ShardKeeperDbValue
}

func (suite *ShardKeeperTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()

	suite.shardKeeper = &shardKeeper{
		service: "foo",
		lg:      lg,

		bridgeLease: noLease,
		guardLease:  noLease,
	}

	db, _ := bolt.Open("4unittest.db", 0600, nil)
	db.Update(
		func(tx *bolt.Tx) error {
			tx.DeleteBucket([]byte(suite.shardKeeper.service))
			tx.CreateBucket([]byte(suite.shardKeeper.service))
			return nil
		},
	)

	suite.shardKeeper.db = db
	suite.curShard = &ShardKeeperDbValue{
		Spec: &ShardSpec{
			Id: "bar",
			Lease: &Lease{
				ID:     100,
				Expire: 100,
			},
		},
		Disp: true,
		Drop: false,
	}

	// 写入初始数据
	suite.shardKeeper.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			b.Put([]byte(suite.curShard.Spec.Id), []byte(suite.curShard.String()))
			return nil
		},
	)
}

func (suite *ShardKeeperTestSuite) TestDropByLease_UnmarshalError() {
	// 写入非法value数据
	suite.shardKeeper.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			b.Put([]byte(mock.Anything), []byte(mock.Anything))
			return nil
		},
	)
	err := suite.shardKeeper.dropBridgeLease(
		&Lease{
			ID:     1,
			Expire: 1,
		},
	)
	assert.NotNil(suite.T(), err)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestDropByLease_IgnoreEqualCase() {
	err := suite.shardKeeper.dropBridgeLease(
		&Lease{
			ID:     1,
			Expire: 1,
		},
	)
	assert.Nil(suite.T(), err)
	suite.shardKeeper.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			v := b.Get([]byte(suite.curShard.Spec.Id))
			var dbValue ShardKeeperDbValue
			json.Unmarshal(v, &dbValue)
			assert.True(suite.T(), dbValue.Disp)
			assert.False(suite.T(), dbValue.Drop)
			return nil
		},
	)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestAdd_create() {
	fakeShardId := "bar"
	err := suite.shardKeeper.Add(fakeShardId, &ShardSpec{Id: fakeShardId})
	assert.Nil(suite.T(), err)
	suite.shardKeeper.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			v := b.Get([]byte(fakeShardId))
			var dbValue ShardKeeperDbValue
			json.Unmarshal(v, &dbValue)
			assert.False(suite.T(), dbValue.Disp)
			assert.False(suite.T(), dbValue.Drop)
			assert.Equal(suite.T(), fakeShardId, dbValue.Spec.Id)
			return nil
		},
	)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestAdd_update() {
	fakeShardId := suite.curShard.Spec.Id
	err := suite.shardKeeper.Add(fakeShardId, &ShardSpec{Id: fakeShardId})
	assert.Nil(suite.T(), err)
	suite.shardKeeper.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			v := b.Get([]byte(fakeShardId))
			var dbValue ShardKeeperDbValue
			json.Unmarshal(v, &dbValue)
			assert.False(suite.T(), dbValue.Disp)
			assert.False(suite.T(), dbValue.Drop)
			assert.Equal(suite.T(), fakeShardId, dbValue.Spec.Id)
			return nil
		},
	)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestDrop_NotExist() {
	fakeShardId := mock.Anything
	err := suite.shardKeeper.Drop(fakeShardId)
	assert.Equal(suite.T(), err, ErrNotExist)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestDrop_success() {
	fakeShardId := suite.curShard.Spec.Id
	err := suite.shardKeeper.Drop(fakeShardId)
	assert.Nil(suite.T(), err)

	suite.shardKeeper.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			v := b.Get([]byte(fakeShardId))
			var dbValue ShardKeeperDbValue
			json.Unmarshal(v, &dbValue)
			assert.False(suite.T(), dbValue.Disp)
			assert.True(suite.T(), dbValue.Drop)
			assert.Equal(suite.T(), fakeShardId, dbValue.Spec.Id)
			return nil
		},
	)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestSync_NotInitializedAndDrop() {
	suite.curShard.Drop = true

	var err error
	err = suite.shardKeeper.db.Update(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			return b.Put([]byte(suite.curShard.Spec.Id), []byte(suite.curShard.String()))
		},
	)
	assert.Nil(suite.T(), err)

	mockedTrigger := new(MockedTrigger)
	mockedTrigger.On("Put", mock.Anything).Return(nil)
	suite.shardKeeper.dispatchTrigger = mockedTrigger

	err = suite.shardKeeper.sync()

	mockedTrigger.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), suite.shardKeeper.initialized)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestDispatch_AddWithNilError() {
	fakeShardId := "bar"
	fakeDV := &ShardKeeperDbValue{
		Spec: &ShardSpec{
			Id: fakeShardId,
		},
	}

	mockedShardInterface := new(MockedShardInterface)
	mockedShardInterface.On("Add", fakeShardId, fakeDV.Spec).Return(nil)
	suite.shardKeeper.shardImpl = mockedShardInterface

	err := suite.shardKeeper.dispatch(addTrigger, fakeDV)
	mockedShardInterface.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
	suite.shardKeeper.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			v := b.Get([]byte(fakeShardId))
			var dbValue ShardKeeperDbValue
			json.Unmarshal(v, &dbValue)
			assert.True(suite.T(), dbValue.Disp)
			assert.False(suite.T(), dbValue.Drop)
			return nil
		},
	)
	suite.shardKeeper.db.Close()
}

func (suite *ShardKeeperTestSuite) TestDispatch_DropWithNilError() {
	fakeShardId := suite.curShard.Spec.Id
	fakeDV := &ShardKeeperDbValue{
		Spec: &ShardSpec{
			Id: fakeShardId,
		},
	}

	mockedShardInterface := new(MockedShardInterface)
	mockedShardInterface.On("Drop", fakeShardId).Return(nil)
	suite.shardKeeper.shardImpl = mockedShardInterface

	err := suite.shardKeeper.dispatch(dropTrigger, fakeDV)
	mockedShardInterface.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
	suite.shardKeeper.db.View(
		func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(suite.shardKeeper.service))
			v := b.Get([]byte(fakeShardId))
			assert.Nil(suite.T(), v)
			return nil
		},
	)
	suite.shardKeeper.db.Close()
}
