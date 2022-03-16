package smserver

import (
	"fmt"
	"testing"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/zd3tl/evtrigger"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestMapperState(t *testing.T) {
	suite.Run(t, new(MapperStateTestSuite))
}

func TestMapper(t *testing.T) {
	suite.Run(t, new(MapperTestSuite))
}

type MapperStateTestSuite struct {
	suite.Suite

	ms *mapperState
}

func (suite *MapperStateTestSuite) SetupTest() {
	suite.ms = newMapperState()
}

func (suite *MapperStateTestSuite) TestForEach() {
	suite.ms.alive["foo"] = newTemporary(time.Now().Unix())

	var expectShardIds []string
	err := suite.ms.ForEach(func(id string, tmp *temporary) error {
		expectShardIds = append(expectShardIds, id)
		return nil
	})
	assert.Nil(suite.T(), err)
	for _, shardId := range expectShardIds {
		_, ok := suite.ms.alive[shardId]
		assert.True(suite.T(), ok)
	}
}

var (
	fakeContainerId = mock.Anything
	fakeShards      = []*apputil.ShardKeeperDbValue{
		{Spec: &apputil.ShardSpec{Id: "foo"}, Lease: 1},
		{Spec: &apputil.ShardSpec{Id: "bar"}, Lease: 1},
	}
)

type MapperTestSuite struct {
	suite.Suite

	mpr *mapper
}

func (suite *MapperTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()

	mpr := mapper{
		lg:      lg,
		appSpec: &smAppSpec{Service: "test"},

		containerState: newMapperState(),
		shardState:     newMapperState(),

		shard: &smShard{
			guardLeaseID: 1,
		},
	}
	trigger, _ := evtrigger.NewTrigger(
		evtrigger.WithLogger(mpr.lg),
		evtrigger.WithWorkerSize(triggerWorkerSize),
	)
	mpr.trigger = trigger

	suite.mpr = &mpr
}

func (suite *MapperTestSuite) createFakeContainer() {
	// 构造测试数据

	fakeHbA := apputil.ContainerHeartbeat{
		Heartbeat: apputil.Heartbeat{
			Timestamp: time.Now().Unix(),
		},
		Shards: fakeShards,
	}
	suite.mpr.create(fakeContainerId, []byte(fakeHbA.String()))
}

func (suite *MapperTestSuite) TestExtractId() {
	var tests = []struct {
		key    string
		expect string
	}{
		{
			key:    "/sm/app/foo/containerhb/bar/694d7e6d1f5e4fb3",
			expect: "bar",
		},
	}
	for _, tt := range tests {
		actual := suite.mpr.extractId(tt.key)
		assert.Equal(suite.T(), actual, tt.expect)
	}
}

func (suite *MapperTestSuite) TestAliveContainers() {
	suite.mpr.containerState.alive["foo"] = new(temporary)
	suite.mpr.containerState.alive["bar"] = new(temporary)

	actual := suite.mpr.AliveContainers()
	assert.Equal(
		suite.T(),
		actual,
		ArmorMap{
			"foo": "",
			"bar": "",
		},
	)
}

func (suite *MapperTestSuite) TestAliveShards() {
	tA := new(temporary)
	tB := new(temporary)
	suite.mpr.shardState.alive["foo"] = tA
	suite.mpr.shardState.alive["bar"] = tB
	actual := suite.mpr.AliveShards()
	assert.Equal(
		suite.T(),
		actual,
		map[string]*temporary{
			"foo": tA,
			"bar": tB,
		},
	)
}

func (suite *MapperTestSuite) TestCreate_shard() {
	hb := apputil.ContainerHeartbeat{
		Heartbeat: apputil.Heartbeat{Timestamp: time.Now().Unix()},
		Shards:    fakeShards,
	}

	containerId := mock.Anything

	err := suite.mpr.create(containerId, []byte(hb.String()))
	assert.Nil(suite.T(), err)

	ctrT := suite.mpr.containerState.alive[containerId]

	assert.Equal(suite.T(), ctrT.lastHeartbeatTime.Unix(), hb.Timestamp)
	for _, t := range suite.mpr.shardState.alive {
		assert.Equal(suite.T(), t.curContainerId, containerId)
		assert.Equal(suite.T(), t.lastHeartbeatTime.Unix(), hb.Timestamp)
	}
}

func (suite *MapperTestSuite) TestDelete_success() {
	fakeT := &temporary{curContainerId: "fakeContainerId"}

	suite.mpr.containerState.alive[mock.Anything] = new(temporary)
	suite.mpr.shardState.alive["foo"] = &temporary{curContainerId: mock.Anything}
	suite.mpr.shardState.alive["bar"] = fakeT

	err := suite.mpr.Delete(mock.Anything)
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), 1, len(suite.mpr.shardState.alive))
	assert.Equal(suite.T(), fakeT, suite.mpr.shardState.alive["bar"])
}

func (suite *MapperTestSuite) TestRefresh_containerNotExist() {
	hb := apputil.ContainerHeartbeat{
		Heartbeat: apputil.Heartbeat{Timestamp: time.Now().Unix()},
		Shards:    fakeShards,
	}
	event := clientv3.Event{
		Kv: &mvccpb.KeyValue{
			Key:   []byte(""),
			Value: []byte(hb.String()),
		},
	}

	fakeContainerId := mock.Anything
	err := suite.mpr.Refresh(fakeContainerId, &event)
	assert.Nil(suite.T(), err)
	ctrT := suite.mpr.containerState.alive[fakeContainerId]
	assert.Equal(suite.T(), ctrT.lastHeartbeatTime.Unix(), hb.Timestamp)
}

func (suite *MapperTestSuite) TestRefresh_containerUpdateTime() {
	suite.createFakeContainer()

	fakeHbB := apputil.ContainerHeartbeat{
		Heartbeat: apputil.Heartbeat{
			Timestamp: time.Now().Unix() + 1,
		},
		Shards: fakeShards,
	}
	event := clientv3.Event{
		Kv: &mvccpb.KeyValue{
			Key:   []byte(""),
			Value: []byte(fakeHbB.String()),
		},
	}
	err := suite.mpr.Refresh(fakeContainerId, &event)
	assert.Nil(suite.T(), err)

	ctrT := suite.mpr.containerState.alive[fakeContainerId]
	assert.Equal(suite.T(), ctrT.lastHeartbeatTime.Unix(), fakeHbB.Timestamp)
	for _, sT := range suite.mpr.shardState.alive {
		assert.Equal(suite.T(), sT.curContainerId, fakeContainerId)
		assert.Equal(suite.T(), sT.lastHeartbeatTime.Unix(), fakeHbB.Timestamp)
	}
}

func (suite *MapperTestSuite) TestWait_containerNotFound() {
	fakeContainerId := mock.Anything
	err := suite.mpr.Wait(fakeContainerId)
	assert.Equal(suite.T(), err, apputil.ErrNotExist)
}

func (suite *MapperTestSuite) TestWait_triggerWait() {
	// 构造测试数据
	fakeContainerId := mock.Anything
	fakeHbA := apputil.ContainerHeartbeat{
		Heartbeat: apputil.Heartbeat{
			Timestamp: time.Now().Unix(),
		},
		Shards: fakeShards,
	}
	err := suite.mpr.create(fakeContainerId, []byte(fakeHbA.String()))
	assert.Nil(suite.T(), err)

	suite.mpr.maxRecoveryTime = 3 * time.Second
	err = suite.mpr.Wait(fakeContainerId)
	assert.Nil(suite.T(), err)
}

func (suite *MapperTestSuite) TestUpdateState_createEvent() {
	suite.createFakeContainer()

	fakeHbB := apputil.ContainerHeartbeat{
		Heartbeat: apputil.Heartbeat{
			Timestamp: time.Now().Unix() + 1,
		},
		Shards: fakeShards,
	}

	// create event
	createEvent := clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			CreateRevision: 0,
			ModRevision:    0,
			Key:            []byte(fmt.Sprintf("/sm/app/foo/containerhb/%s/694d7e6d1f5e4fb3", fakeContainerId)),
			Value:          []byte(fakeHbB.String()),
		},
	}
	err := suite.mpr.UpdateState(mock.Anything, &createEvent)
	assert.Nil(suite.T(), err)
	ctrT := suite.mpr.containerState.alive[fakeContainerId]
	assert.Equal(suite.T(), ctrT.lastHeartbeatTime.Unix(), fakeHbB.Heartbeat.Timestamp)
}

func (suite *MapperTestSuite) TestUpdateState_deleteEvent() {
	suite.createFakeContainer()

	// delete event
	deleteEvent := clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key: []byte(fmt.Sprintf("/sm/app/foo/containerhb/%s/694d7e6d1f5e4fb3", fakeContainerId)),
		},
	}

	err := suite.mpr.UpdateState(mock.Anything, &deleteEvent)
	assert.Nil(suite.T(), err)
}
