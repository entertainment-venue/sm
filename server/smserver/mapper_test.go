package smserver

import (
	"encoding/json"
	"fmt"
	"github.com/entertainment-venue/sm/pkg/apputil"
	"go.uber.org/zap"
	"testing"
	"time"
)

func Test_mapperState_Create(t *testing.T) {
	lg, _ := zap.NewDevelopment()
	mpr := mapper{
		lg:      lg,
		appSpec: &smAppSpec{Service: "test"},
	}

	mprs := newMapperState(&mpr, containerTrigger)

	hb := apputil.Heartbeat{Timestamp: time.Now().Unix()}
	b, _ := json.Marshal(hb)
	mprs.Create("foo", b)
}

func Test_mapperState_Delete(t *testing.T) {
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

	mprs.Delete("foo")
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
