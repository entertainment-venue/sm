package storage

import (
	"testing"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EtcdTestSuite struct {
	suite.Suite

	db *etcddb
}

func TestEtcd(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()
	service := "foo"
	containerId := "127.0.0.1:8801"
	suite.db, _ = NewEtcddb(service, containerId, nil, lg)
}

func (suite *EtcdTestSuite) TestAdd() {
	shard := &ShardSpec{Id: mock.Anything}
	shardPath := etcdutil.ShardPath(suite.db.service, suite.db.containerId, shard.Id)

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("GetKV", mock.Anything, shardPath, mock.Anything).Return(&clientv3.GetResponse{}, nil)
	mockedEtcdWrapper.On("UpdateKV", mock.Anything, shardPath, mock.Anything).Return(nil)
	suite.db.client = mockedEtcdWrapper

	err := suite.db.Add(shard)
	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)

	assert.Equal(suite.T(), suite.db.mu.kvs[shard.Id], &ShardKeeperDbValue{Spec: shard})
}

func (suite *EtcdTestSuite) TestDrop() {
	var err error

	err = suite.db.Drop(nil)
	assert.Nil(suite.T(), err)

	suite.db.mu.kvs[mock.Anything] = &ShardKeeperDbValue{Spec: &ShardSpec{}}

	// 测试下过滤逻辑
	suite.db.mu.kvs["foo"] = &ShardKeeperDbValue{Spec: &ShardSpec{}}

	err = suite.db.Drop([]string{mock.Anything})
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), suite.db.mu.kvs[mock.Anything].Drop)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Disp)

	assert.False(suite.T(), suite.db.mu.kvs["foo"].Disp)
	assert.False(suite.T(), suite.db.mu.kvs["foo"].Drop)
}

func (suite *EtcdTestSuite) TestMigrateLease() {
	suite.db.mu.kvs[mock.Anything] = &ShardKeeperDbValue{
		Spec: &ShardSpec{
			Lease: &Lease{
				ID: 100,
			},
		},
	}

	// ok
	shardPath := etcdutil.ShardPath(suite.db.service, suite.db.containerId, mock.Anything)
	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("UpdateKV", mock.Anything, shardPath, mock.Anything).Return(nil)
	suite.db.client = mockedEtcdWrapper
	err := suite.db.MigrateLease(100, 101)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), suite.db.mu.kvs[mock.Anything].Spec.Lease.ID, clientv3.LeaseID(101))

	// not ok
	err = suite.db.MigrateLease(102, 103)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), suite.db.mu.kvs[mock.Anything].Drop)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Disp)
}

func (suite *EtcdTestSuite) TestDropByLease() {
	suite.db.mu.kvs[mock.Anything] = &ShardKeeperDbValue{
		Spec: &ShardSpec{
			Lease: &Lease{
				ID: 100,
			},
		},
	}

	// 干掉100之外的lease
	err := suite.db.DropByLease(true, clientv3.LeaseID(100))
	assert.Nil(suite.T(), err)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Drop)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Disp)

	err = suite.db.DropByLease(true, clientv3.LeaseID(101))
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), suite.db.mu.kvs[mock.Anything].Drop)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Disp)

	// reset
	suite.db.mu.kvs[mock.Anything] = &ShardKeeperDbValue{
		Spec: &ShardSpec{
			Lease: &Lease{
				ID: 100,
			},
		},
	}

	err = suite.db.DropByLease(false, clientv3.LeaseID(101))
	assert.Nil(suite.T(), err)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Drop)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Disp)

	err = suite.db.DropByLease(false, clientv3.LeaseID(100))
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), suite.db.mu.kvs[mock.Anything].Drop)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Disp)
}

func (suite *EtcdTestSuite) TestReset() {
	dv := ShardKeeperDbValue{
		Spec: &ShardSpec{
			Lease: &Lease{
				ID: 100,
			},
		},
		Disp: true,
	}
	kvs := make(map[string]string)
	kvs[mock.Anything] = dv.String()

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("GetKVs", mock.Anything, etcdutil.ShardDir(suite.db.service, suite.db.containerId)).Return(kvs, nil)
	suite.db.client = mockedEtcdWrapper

	err := suite.db.Reset()
	assert.Nil(suite.T(), err)
	assert.False(suite.T(), suite.db.mu.kvs[mock.Anything].Disp)
}