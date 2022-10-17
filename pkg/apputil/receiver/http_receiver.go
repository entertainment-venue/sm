package receiver

import (
	"context"
	"net/http"

	"github.com/entertainment-venue/sm/pkg/apputil/core"
	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"github.com/entertainment-venue/sm/pkg/commonutil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var _ Receiver = new(httpReceiver)

type httpReceiver struct {
	lg          *zap.Logger
	addr        string
	containerId string
	shardKeeper core.ShardPrimitives

	ginEngine *gin.Engine
	svr       *http.Server
}

type HttpReceiverRequest struct {
	Id   string             `json:"id"`
	Spec *storage.ShardSpec `json:"spec"`
}

func NewHttpServer(lg *zap.Logger, addr string, containerId string) *httpReceiver {
	svr := httpReceiver{
		lg:          lg,
		addr:        addr,
		containerId: containerId,

		ginEngine: gin.Default(),
	}
	routerGroup := svr.ginEngine.Group("/sm/admin")
	{
		routerGroup.POST("/add-shard", svr.AddShard)
		routerGroup.POST("/drop-shard", svr.DropShard)
	}
	return &svr
}

func (r *httpReceiver) Start() error {
	r.svr = &http.Server{
		Addr:    r.addr,
		Handler: r.ginEngine,
	}
	go func() {
		if err := r.svr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			r.lg.Panic(
				"ListenAndServe err",
				zap.String("addr", r.addr),
				zap.Error(err),
			)
			return
		}
		r.lg.Info(
			"ListenAndServe exit",
			zap.String("addr", r.addr),
		)
	}()
	return nil
}

func (r *httpReceiver) Shutdown() error {
	if err := r.svr.Shutdown(context.TODO()); err != nil {
		r.lg.Error(
			"Shutdown error",
			zap.String("addr", r.addr),
			zap.Error(err),
		)
		return errors.Wrap(err, "")
	}
	r.lg.Info(
		"Shutdown success",
		zap.String("addr", r.addr),
	)
	return nil
}

func (r *httpReceiver) Extract() interface{} {
	return r.ginEngine
}

func (r *httpReceiver) SetShardPrimitives(sp core.ShardPrimitives) {
	r.shardKeeper = sp
}

func (r *httpReceiver) AddShard(c *gin.Context) {
	var req HttpReceiverRequest
	if err := c.ShouldBind(&req); err != nil {
		r.lg.Error("ShouldBind err", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// shard属性校验
	if err := req.Spec.Validate(); err != nil {
		r.lg.Error(
			"Validate err",
			zap.Reflect("req", req),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// container校验
	if req.Spec.ManualContainerId != "" && req.Spec.ManualContainerId != r.containerId {
		r.lg.Error(
			"ManualContainerId not match",
			zap.Reflect("req", req),
			zap.String("server-container-id", r.containerId),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "unexpected container"})
		return
	}

	req.Spec.Id = req.Id
	if err := r.shardKeeper.Add(req.Id, req.Spec); err != nil {
		r.lg.Error(
			"shardKeeper Add err",
			zap.Reflect("req", req),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	r.lg.Info(
		"add shard success",
		zap.Reflect("req", req),
	)

	c.JSON(http.StatusOK, gin.H{})
}

func (r *httpReceiver) DropShard(c *gin.Context) {
	var req HttpReceiverRequest
	if err := c.ShouldBind(&req); err != nil {
		r.lg.Error(
			"ShouldBind err",
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := r.shardKeeper.Drop(req.Id); err != nil && err != commonutil.ErrNotExist {
		r.lg.Error(
			"Drop err",
			zap.Error(err),
			zap.String("id", req.Id),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	r.lg.Info(
		"drop shard success",
		zap.Reflect("req", req),
	)
	c.JSON(http.StatusOK, gin.H{})
}
