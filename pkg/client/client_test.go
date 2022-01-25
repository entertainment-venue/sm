package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
)

func TestStartSM(t *testing.T) {
	port := 8888
	ginSrv := gin.Default()
	if err := StartSM(SmWithRouter(ginSrv),
		SmWithContainerId(fmt.Sprintf("%s:%d", getLocalIP(), port)),
		SmWithEtcdAddr([]string{"10.189.73.122:8989"}),
		SmWithService("proxy.dev"),
		SmWithImplementation(&testShard{ids: make(map[string]string)})); err != nil {
		t.Fatal(err)
	}
	_ = ginSrv.Run(fmt.Sprintf(":%d", port))
}

type testShard struct {
	lock sync.Mutex
	ids  map[string]string
}

func (s *testShard) Add(ctx context.Context, id string, spec *apputil.ShardSpec) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ids[id] = ""
	fmt.Printf("add op for %s %+v\n", id, *spec)
	return nil
}

func (s *testShard) Drop(ctx context.Context, id string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.ids, id)
	fmt.Printf("drop op %s\n", id)
	return nil
}

func (s *testShard) Load(ctx context.Context, id string) (string, error) {
	fmt.Printf("load op %s\n", id)
	return "", nil
}

func (s *testShard) Shards(ctx context.Context) ([]string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var r []string
	for id, _ := range s.ids {
		r = append(r, id)
	}
	fmt.Printf("shards op %s\n", r)
	return r, nil
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Printf("get local IP failed, error is %+v\n", err)
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
