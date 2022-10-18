package storage

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type BoltdbTestSuite struct {
	suite.Suite

	db      *boltdb
	shardId string
	spec    *ShardSpec
}

func TestBoltdb(t *testing.T) {
	suite.Run(t, new(BoltdbTestSuite))
}

func (suite *BoltdbTestSuite) SetupTest() {
	service := "foo"
	suite.db, _ = NewBoltdb("./", service)
	suite.shardId = "bar"

	spec := ShardSpec{
		Id: suite.shardId,
		Lease: &Lease{
			ID: 1,
		},
	}
	suite.spec = &spec

	var err error
	err = suite.db.Add(&spec)
	assert.Nil(suite.T(), err)
}

func (suite *BoltdbTestSuite) TestAdd() {
	var err error

	// 重复添加不报错，有日志
	err = suite.db.Add(suite.spec)
	assert.Nil(suite.T(), err)

	v, _ := suite.db.Get([]byte(suite.shardId))
	var dbValue ShardKeeperDbValue
	json.Unmarshal(v, &dbValue)
	assert.True(suite.T(), reflect.DeepEqual(*suite.spec, *dbValue.Spec))

	suite.db.Clear()
	suite.db.Close()
}

func (suite *BoltdbTestSuite) TestDrop() {
	var err error

	// empty ids
	err = suite.db.Drop([]string{})
	assert.Nil(suite.T(), err)

	// not exist id
	err = suite.db.Drop([]string{mock.Anything})
	assert.Nil(suite.T(), err)

	// drop succ
	err = suite.db.Drop([]string{suite.shardId})
	assert.Nil(suite.T(), err)
	// check data in boltdb
	v, _ := suite.db.Get([]byte(suite.shardId))
	var dbValue ShardKeeperDbValue
	json.Unmarshal(v, &dbValue)
	assert.False(suite.T(), dbValue.Disp)
	assert.True(suite.T(), dbValue.Drop)

	suite.db.Clear()
	suite.db.Close()
}

func (suite *BoltdbTestSuite) TestMigrateLease() {
	// already to
	var err error
	err = suite.db.MigrateLease(1, clientv3.NoLease)
	assert.Nil(suite.T(), err)

	var (
		v       []byte
		dbValue ShardKeeperDbValue
	)

	// migrate success
	err = suite.db.MigrateLease(clientv3.NoLease, 1)
	assert.Nil(suite.T(), err)
	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	assert.Equal(suite.T(), clientv3.LeaseID(1), dbValue.Spec.Lease.ID)

	// drop exception
	err = suite.db.MigrateLease(2, 3)
	assert.Nil(suite.T(), err)
	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	assert.Equal(suite.T(), clientv3.LeaseID(1), dbValue.Spec.Lease.ID)
	assert.False(suite.T(), dbValue.Disp)
	assert.True(suite.T(), dbValue.Drop)

	suite.db.Clear()
	suite.db.Close()
}

func (suite *BoltdbTestSuite) TestDropByLease_trueExclude() {
	// test exclude = true
	var (
		err     error
		v       []byte
		dbValue ShardKeeperDbValue
	)

	// drop fail
	err = suite.db.DropByLease(true, 1)
	assert.Nil(suite.T(), err)
	// check drop fail
	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	assert.False(suite.T(), dbValue.Drop)
	assert.False(suite.T(), dbValue.Disp)

	// drop succ
	err = suite.db.DropByLease(true, 2)
	assert.Nil(suite.T(), err)
	// soft delete
	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	assert.True(suite.T(), dbValue.Drop)
	assert.False(suite.T(), dbValue.Disp)

	suite.db.Clear()
	suite.db.Close()
}

func (suite *BoltdbTestSuite) TestDropByLease_falseExclude() {
	var (
		err     error
		v       []byte
		dbValue ShardKeeperDbValue
	)

	// test exclude = false

	// drop fail
	err = suite.db.DropByLease(false, 2)
	assert.Nil(suite.T(), err)
	// check drop fail
	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	assert.False(suite.T(), dbValue.Drop)
	assert.False(suite.T(), dbValue.Disp)

	// drop succ
	err = suite.db.DropByLease(false, 1)
	assert.Nil(suite.T(), err)
	// check soft delete
	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	assert.True(suite.T(), dbValue.Drop)
	assert.False(suite.T(), dbValue.Disp)

	suite.db.Clear()
	suite.db.Close()
}

func (suite *BoltdbTestSuite) TestReset() {
	var (
		err     error
		v       []byte
		dbValue ShardKeeperDbValue
	)

	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	dbValue.Disp = true
	suite.db.Update([]byte(suite.shardId), []byte(dbValue.String()))

	err = suite.db.Reset()
	assert.Nil(suite.T(), err)
	v, _ = suite.db.Get([]byte(suite.shardId))
	json.Unmarshal(v, &dbValue)
	assert.False(suite.T(), dbValue.Disp)

	suite.db.Clear()
	suite.db.Close()
}
