package smserver

import (
	"context"
	"fmt"
	"sync"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"go.uber.org/zap"
)

// 启动被管理的sharded application
func newTestShardServer() (*apputil.Container, *apputil.ShardServer, context.CancelFunc) {
	service := "foo.bar2"

	ctx, cancel := context.WithCancel(context.TODO())

	logger, _ := zap.NewProduction()

	c, err := apputil.NewContainer(
		apputil.ContainerWithContext(ctx),
		apputil.ContainerWithService(service),
		apputil.ContainerWithId("127.0.0.1:8802"),
		apputil.ContainerWithEndpoints([]string{"127.0.0.1:2379"}),
		apputil.ContainerWithLogger(logger))
	if err != nil {
		panic(err)
	}

	ss, err := apputil.NewShardServer(
		apputil.ShardServerWithAddr(":8802"),
		apputil.ShardServerWithContext(ctx),
		apputil.ShardServerWithContainer(c),
		apputil.ShardServerWithShardImplementation(&testShard{m: make(map[string]string)}),
		apputil.ShardServerWithLogger(logger))
	if err != nil {
		panic(err)
	}

	return c, ss, cancel
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
