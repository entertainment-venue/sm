package smserver

import (
	"testing"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

func TestContainer(t *testing.T) {
	suite.Run(t, new(ContainerTestSuite))
}

type MockedShardWrapper struct {
	mock.Mock
}

func (m *MockedShardWrapper) NewShard(c *smContainer, spec *apputil.ShardSpec) (Shard, error) {
	args := m.Called(c, spec)
	return args.Get(0).(Shard), args.Error(1)
}

type MockedShard struct {
	mock.Mock
}

func (m *MockedShard) SetMaxShardCount(maxShardCount int) {
	m.Called(maxShardCount)
}

func (m *MockedShard) SetMaxRecoveryTime(maxRecoveryTime int) {
	m.Called(maxRecoveryTime)
}

func (m *MockedShard) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockedShard) Spec() *apputil.ShardSpec {
	args := m.Called()
	return args.Get(0).(*apputil.ShardSpec)
}

func (m *MockedShard) Load() string {
	args := m.Called()
	return args.String(0)
}

type ContainerTestSuite struct {
	suite.Suite

	container *smContainer
}

func (suite *ContainerTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()
	suite.container = &smContainer{
		lg:        lg,
		Container: &apputil.Container{},
		stopper:   &apputil.GoroutineStopper{},
		shards:    make(map[string]Shard),
	}
	suite.container.shards["s1"] = &smShard{
		shardSpec: &apputil.ShardSpec{Id: "s1"},
	}
}

func (suite *ContainerTestSuite) TestGetShard() {
	s, err := suite.container.GetShard("s1")
	assert.Nil(suite.T(), err)
	if assert.NotNil(suite.T(), s) {
		assert.Equal(suite.T(), s.Spec().Id, "s1")
	}
}

func (suite *ContainerTestSuite) TestAdd_closing() {
	// errClosing test
	var err error
	suite.container.closing = true
	err = suite.container.Add("s2", &apputil.ShardSpec{})
	assert.Equal(suite.T(), err, apputil.ErrClosing)

	// errExist test
	suite.container.closing = false
	err = suite.container.Add("s1", &apputil.ShardSpec{})
	assert.Equal(suite.T(), err, apputil.ErrExist)
}

func (suite *ContainerTestSuite) TestAdd_create() {
	// mock
	fakeSpec := &apputil.ShardSpec{}
	fakeShard := &smShard{shardSpec: fakeSpec}
	mockedShardWrapper := new(MockedShardWrapper)
	mockedShardWrapper.On("NewShard", suite.container, fakeSpec).Return(fakeShard, nil)

	// call
	suite.container.shardWrapper = mockedShardWrapper
	err := suite.container.Add("s2", fakeSpec)

	// assert
	mockedShardWrapper.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), suite.container.shards["s2"], fakeShard)
}

func (suite *ContainerTestSuite) TestAdd_update() {
	paramSpec := &apputil.ShardSpec{}

	fakeSpec := &apputil.ShardSpec{Task: "foo"}

	// mock
	mockedShard := new(MockedShard)
	mockedShard.On("Spec").Return(fakeSpec)
	mockedShard.On("Close").Return(nil)
	suite.container.shards["s2"] = mockedShard

	// mock ShardWrapper
	mockedShardWrapper := new(MockedShardWrapper)
	mockedShardWrapper.On("NewShard", suite.container, paramSpec).Return(mockedShard, nil)

	// call
	suite.container.shardWrapper = mockedShardWrapper
	err := suite.container.Add("s2", paramSpec)

	// assert
	mockedShard.AssertExpectations(suite.T())
	mockedShardWrapper.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ContainerTestSuite) TestDrop_closing() {
	suite.container.closing = true
	err := suite.container.Drop("s1")
	assert.Equal(suite.T(), err, apputil.ErrClosing)
}

func (suite *ContainerTestSuite) TestDrop_notExist() {
	err := suite.container.Drop(mock.Anything)
	assert.Equal(suite.T(), err, apputil.ErrNotExist)
}

func (suite *ContainerTestSuite) TestDrop_common() {
	mockedShard := new(MockedShard)
	mockedShard.On("Close").Return(nil)
	suite.container.shards["s2"] = mockedShard

	err := suite.container.Drop("s2")
	mockedShard.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
}

func (suite *ContainerTestSuite) TestLoad_closing() {
	suite.container.closing = true
	_, err := suite.container.Load("s1")
	assert.Equal(suite.T(), err, apputil.ErrClosing)
}

func (suite *ContainerTestSuite) TestLoad_notExist() {
	_, err := suite.container.Load(mock.Anything)
	assert.Equal(suite.T(), apputil.ErrNotExist, err)
}

func (suite *ContainerTestSuite) TestLoad_common() {
	mockedShard := new(MockedShard)
	mockedShard.On("Load").Return(mock.Anything, nil)
	suite.container.shards["s2"] = mockedShard

	str, err := suite.container.Load("s2")
	mockedShard.AssertExpectations(suite.T())
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), str, mock.Anything)
}

func (suite *ContainerTestSuite) TestClose_closing() {
	suite.container.closing = true
	err := suite.container.Close()
	assert.Equal(suite.T(), err, apputil.ErrClosing)
}
