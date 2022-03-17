package client

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
)

func TestNewClient(t *testing.T) {
	port := 8888
	ginSrv := gin.Default()
	_, err := NewClient(
		ClientWithRouter(ginSrv),
		ClientWithContainerId(fmt.Sprintf("%s:%d", "127.0.0.1", port)),
		ClientWithEtcdAddr([]string{"127.0.0.1:2379"}),
		ClientWithService("test-service"),
		ClientWithImplementation(&testShard{ids: make(map[string]string)}),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = ginSrv.Run(fmt.Sprintf(":%d", port))
}

type testShard struct {
	lock sync.Mutex
	ids  map[string]string
}

func (s *testShard) Add(id string, spec *apputil.ShardSpec) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ids[id] = ""
	fmt.Printf("add op for %s %+v\n", id, *spec)
	return nil
}

func (s *testShard) Drop(id string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.ids, id)
	fmt.Printf("drop op %s\n", id)
	return nil
}
