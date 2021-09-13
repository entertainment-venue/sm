package client

import (
	"context"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/entertainment-venue/borderland/pkg/apputil"
	"github.com/entertainment-venue/borderland/pkg/logutil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

type ShardOpMessage struct {
	Id   string `json:"id"`
	Type OpType `json:"type"`
}

type OpType int

const (
	OpTypeAdd OpType = iota
	OpTypeDrop
)

// 直接帮助接入方把服务器端启动好，引入gin框架，和sarama sdk的接入方式相似，提供消息的chan或者callback func给到接入app的业务逻辑
type ShardServer struct {
	impl      apputil.ShardInterface
	shards    map[string]struct{}
	container *apputil.Container
}

type shardServerOptions struct {
	addr           string
	implementation apputil.ShardInterface
	container      *apputil.Container
}

type ShardServerOption func(options *shardServerOptions)

func WithAddr(v string) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.addr = v
	}
}

func WithContainer(v *apputil.Container) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.container = v
	}
}

func WithShardImplementation(v apputil.ShardInterface) ShardServerOption {
	return func(sso *shardServerOptions) {
		sso.implementation = v
	}
}

func NewShardServer(opts ...ShardServerOption) error {
	ops := &shardServerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	if ops.addr == "" {
		return errors.New("addr err")
	}

	ss := ShardServer{
		shards: make(map[string]struct{}),
	}

	r := gin.Default()
	ssg := r.Group("/bl/shardserver")
	{
		ssg.POST("/add-shard", ss.GinShardServerAddShard)
		ssg.POST("/drop-shard", ss.GinContainerDropShard)
	}

	if err := r.Run(ops.addr); err != nil {
		return errors.Wrap(err, "")
	}

	// 上传shard的load，load是从接入服务拿到
	go func() {
		apputil.TickerLoop(
			context.TODO(),
			3*time.Second,
			"[shardserver] load uploader exit",
			func(ctx context.Context) error {
				for id := range ss.shards {
					sl, err := ss.impl.Load(context.TODO(), id)
					if err != nil {
						return errors.Wrap(err, "")
					}

					if _, err := ss.container.Client.Put(ctx, apputil.EtcdPathAppShardHbId(ss.container.Service(), ss.container.Id()), sl, clientv3.WithLease(ss.container.Session.Lease())); err != nil {
						return errors.Wrap(err, "")
					}

					// TODO 迁移server的部分功能过来
				}
				return nil
			},
		)
	}()

	return nil
}

func (g *ShardServer) GinShardServerAddShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		logutil.Logger.Printf("err: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeAdd
	logutil.Logger.Printf("[shardserver] add shard req: %+v", req)

	if err := g.impl.Add(context.TODO(), req.Id); err != nil {
		logutil.Logger.Printf("err: %s", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{})
}

func (g *ShardServer) GinContainerDropShard(c *gin.Context) {
	var req ShardOpMessage
	if err := c.ShouldBind(&req); err != nil {
		logutil.Logger.Printf("err: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	req.Type = OpTypeDrop
	logutil.Logger.Printf("[shardserver] add shard req: %+v", req)

	if err := g.impl.Drop(context.TODO(), req.Id); err != nil {
		logutil.Logger.Printf("err: %s", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{})
}
