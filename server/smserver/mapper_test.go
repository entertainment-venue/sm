package smserver

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

func TestMapperState(t *testing.T) {
	suite.Run(t, new(MapperStateTestSuite))
}

type MapperStateTestSuite struct {
	suite.Suite

	ms *mapperState
}

func (suite *MapperStateTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()
	mapper := mapper{
		lg:      lg,
		appSpec: &smAppSpec{Service: "test"},
	}
	suite.ms = newMapperState(&mapper, mock.Anything)
}

func (suite *MapperStateTestSuite) TestCreate_shard() {
	hb := apputil.ShardHeartbeat{
		Heartbeat:   apputil.Heartbeat{Timestamp: time.Now().Unix()},
		Load:        "Load",
		ContainerId: "ContainerId",
	}
	suite.ms.typ = shardTrigger

	err := suite.ms.create(mock.Anything, []byte(hb.String()))
	assert.Nil(suite.T(), err)

	t := suite.ms.alive[mock.Anything]
	assert.Equal(suite.T(), t.lastHeartbeatTime.Unix(), hb.Timestamp)
	assert.Equal(suite.T(), t.curContainerId, hb.ContainerId)
}

func (suite *MapperStateTestSuite) TestCreate_container() {
	hb := apputil.Heartbeat{
		Timestamp: time.Now().Unix(),
	}
	suite.ms.typ = containerTrigger

	err := suite.ms.create(mock.Anything, []byte(hb.String()))
	assert.Nil(suite.T(), err)
	t := suite.ms.alive[mock.Anything]
	assert.Equal(suite.T(), t.lastHeartbeatTime.Unix(), hb.Timestamp)
	assert.Empty(suite.T(), t.curContainerId)
}

func (suite *MapperStateTestSuite) TestDelete_success() {
	suite.ms.alive[mock.Anything] = new(temporary)
	err := suite.ms.Delete(mock.Anything)
	assert.Nil(suite.T(), err)
	t := suite.ms.alive[mock.Anything]
	assert.Nil(suite.T(), t)
}

func (suite *MapperStateTestSuite) TestRefresh_containerNotExist() {
	hb := apputil.Heartbeat{
		Timestamp: time.Now().Unix(),
	}
	suite.ms.typ = containerTrigger

	err := suite.ms.Refresh(mock.Anything, []byte(hb.String()))
	assert.Nil(suite.T(), err)
	t := suite.ms.alive[mock.Anything]
	assert.Equal(suite.T(), t.lastHeartbeatTime.Unix(), hb.Timestamp)
	assert.Empty(suite.T(), t.curContainerId)
}

func (suite *MapperStateTestSuite) TestRefresh_shardNotExist() {
	hb := apputil.ShardHeartbeat{
		Heartbeat:   apputil.Heartbeat{Timestamp: time.Now().Unix()},
		Load:        "Load",
		ContainerId: "ContainerId",
	}
	suite.ms.typ = shardTrigger

	err := suite.ms.Refresh(mock.Anything, []byte(hb.String()))
	assert.Nil(suite.T(), err)
	t := suite.ms.alive[mock.Anything]
	assert.Equal(suite.T(), t.lastHeartbeatTime.Unix(), hb.Timestamp)
	assert.Equal(suite.T(), t.curContainerId, hb.ContainerId)
}

func (suite *MapperStateTestSuite) TestRefresh_containerUpdateTime() {
	// TODO
}

func (suite *MapperStateTestSuite) TestRefresh_shardUpdateTime() {
	// TODO
}

func Test_mapperState_Refresh(t *testing.T) {
	lg, _ := zap.NewDevelopment()
	mpr := mapper{
		lg:      lg,
		appSpec: &smAppSpec{Service: "test"},
	}

	mprs := newMapperState(&mpr, containerTrigger)

	// 4 test
	hb := apputil.Heartbeat{Timestamp: time.Now().Unix()}
	b, _ := json.Marshal(hb)
	mprs.Create("foo", b)

	time.Sleep(time.Second)

	hb2 := apputil.Heartbeat{Timestamp: time.Now().Unix()}
	b2, _ := json.Marshal(hb2)
	mprs.Refresh("foo", b2)
}

func Test_mapperState_ForEach(t *testing.T) {
	lg, _ := zap.NewDevelopment()
	mpr := mapper{
		lg:      lg,
		appSpec: &smAppSpec{Service: "test"},
	}

	mprs := newMapperState(&mpr, containerTrigger)

	// 4 test
	hb := apputil.Heartbeat{Timestamp: time.Now().Unix()}
	b, _ := json.Marshal(hb)
	mprs.Create("foo", b)

	f := func(id string, tmp *temporary) error {
		fmt.Println(id, tmp.lastHeartbeatTime)
		return nil
	}
	mprs.ForEach(f)
}

func Test_mapperState_Wait(t *testing.T) {
	lg, _ := zap.NewDevelopment()
	mpr := mapper{
		lg:              lg,
		appSpec:         &smAppSpec{Service: "test"},
		maxRecoveryTime: 30 * time.Second,
	}

	mprs := newMapperState(&mpr, containerTrigger)

	// 4 test
	hb := apputil.Heartbeat{Timestamp: time.Now().Unix()}
	b, _ := json.Marshal(hb)
	mprs.Create("foo", b)

	mprs.Wait("foo")
}

func Test_mapState_Prefix(t *testing.T) {

}
