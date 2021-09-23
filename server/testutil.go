package server

import (
	"context"
	"fmt"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func newTestShardServer() (*apputil.Container, *apputil.ShardServer, context.CancelFunc) {
	service := "foo.bar"

	ctx, cancel := context.WithCancel(context.TODO())

	logger, _ := zap.NewProduction()

	c, err := apputil.NewContainer(
		apputil.ContainerWithContext(ctx),
		apputil.ContainerWithService(service),
		apputil.ContainerWithId("127.0.0.1:8888"),
		apputil.ContainerWithEndpoints([]string{"127.0.0.1:2379"}),
		apputil.ContainerWithLogger(logger))
	if err != nil {
		panic(err)
	}

	ss, err := apputil.NewShardServer(
		apputil.ShardServerWithAddr(":8889"),
		apputil.ShardServerWithContext(ctx),
		apputil.ShardServerWithContainer(c),
		apputil.ShardServerWithShardImplementation(&testShard{}),
		apputil.ShardServerWithLogger(logger),
		apputil.ShardServerWithShardOpReceiver(&testShard{}))
	if err != nil {
		panic(err)
	}

	return c, ss, cancel
}

type testShard struct{}

func (s *testShard) Add(ctx context.Context, id string, spec *apputil.ShardSpec) error {
	fmt.Println("add op")
	return nil
}

func (s *testShard) Drop(ctx context.Context, id string) error {
	fmt.Println("drop op")
	return nil
}

func (s *testShard) Load(ctx context.Context, id string) (string, error) {
	fmt.Println("load op")
	return "", nil
}

func (s *testShard) Shards(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (s *testShard) AddShard(c *gin.Context) {
	fmt.Println("receive add request")
	return
}

func (s *testShard) DropShard(c *gin.Context) {
	fmt.Println("receive drop request")
	return
}
