package smserver

import (
	"context"
	"fmt"
	"sync"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"go.uber.org/zap"
)

// 启动被管理的sharded application
func newTestShardServer(service string, containerId string, endpoints []string, addr string) (*apputil.Container, *apputil.ShardServer) {
	logger, _ := zap.NewProduction()

	c, err := apputil.NewContainer(
		apputil.ContainerWithService(service),
		apputil.ContainerWithId(containerId),
		apputil.ContainerWithEndpoints(endpoints),
		apputil.ContainerWithLogger(logger))
	if err != nil {
		panic(err)
	}

	ss, err := apputil.NewShardServer(
		apputil.ShardServerWithAddr(addr),
		apputil.ShardServerWithContainer(c),
		apputil.ShardServerWithShardImplementation(&testShard{m: make(map[string]string)}),
		apputil.ShardServerWithLogger(logger))
	if err != nil {
		panic(err)
	}

	return c, ss
}

type testShard struct {
	mu sync.Mutex
	m  ArmorMap
}

func (s *testShard) Add(ctx context.Context, id string, spec *apputil.ShardSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[id] = ""

	fmt.Printf("add op for %s %+v\n", id, *spec)
	return nil
}

func (s *testShard) Drop(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.m, id)

	fmt.Printf("drop op %s\n", id)
	return nil
}

func (s *testShard) Load(ctx context.Context, id string) (string, error) {
	fmt.Printf("load op %s\n", id)
	return "", nil
}

func (s *testShard) Shards(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m.KeyList(), nil
}
