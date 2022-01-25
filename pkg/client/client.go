package client

import (
	"context"
	"fmt"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Client struct {

}

type smOptions struct {
	g           *gin.Engine
	service     string
	containerId string
	etcdPrefix  string
	etcdAddr    []string
	v           apputil.ShardInterface
}

var defaultSmPrefix = "/sm"

type SmOption func(options *smOptions)

func SmWithRouter(g *gin.Engine) SmOption {
	return func(so *smOptions) {
		so.g = g
	}
}

func SmWithService(service string) SmOption {
	return func(so *smOptions) {
		so.service = service
	}
}

func SmWithContainerId(containerId string) SmOption {
	return func(so *smOptions) {
		so.containerId = containerId
	}
}

func SmWithEtcdPrefix(etcdPrefix string) SmOption {
	return func(so *smOptions) {
		so.etcdPrefix = etcdPrefix
	}
}

func SmWithEtcdAddr(etcdAddr []string) SmOption {
	return func(so *smOptions) {
		so.etcdAddr = etcdAddr
	}
}

func SmWithImplementation(v apputil.ShardInterface) SmOption {
	return func(so *smOptions) {
		so.v = v
	}
}

func StartSM(opts ...SmOption) error {
	ops := &smOptions{}
	for _, opt := range opts {
		opt(ops)
	}
	if ops.g == nil {
		return errors.New("gin router empty")
	}
	if ops.service == "" {
		return errors.New("service empty")
	}
	if ops.containerId == "" {
		return errors.New("containerId empty")
	}
	if ops.etcdPrefix == "" {
		ops.etcdPrefix = defaultSmPrefix
	}
	if ops.etcdAddr == nil {
		return errors.New("etcdAddr empty")
	}
	if ops.v == nil {
		return errors.New("impl empty")
	}
	server, err := newServer(ops)
	if err != nil {
		return err
	}
	go loop(server, ops)
	return nil
}

func loop(server *apputil.ShardServer, ops *smOptions) {
	var err error
	for {
		select {
		case <-server.Done():
			for {
				server, err = newServer(ops)
				if err != nil {
					fmt.Printf("sm newServer error: %v\n", err)
					time.Sleep(3 * time.Second)
				} else {
					break
				}
			}
		}
	}
}

func newServer(ops *smOptions) (*apputil.ShardServer, error) {
	ctx, _ := context.WithCancel(context.TODO())
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	c, err := apputil.NewContainer(
		apputil.ContainerWithContext(ctx),
		apputil.ContainerWithService(ops.service),
		apputil.ContainerWithId(ops.containerId),
		apputil.ContainerWithEndpoints(ops.etcdAddr),
		apputil.ContainerWithLogger(logger))
	if err != nil {
		c.Close()
		return nil, err
	}

	ss, err := apputil.NewShardServer(
		apputil.ShardServerWithEtcdPrefix(ops.etcdPrefix),
		apputil.ShardServerWithRouter(ops.g),
		apputil.ShardServerWithContext(ctx),
		apputil.ShardServerWithContainer(c),
		apputil.ShardServerWithShardImplementation(ops.v),
		apputil.ShardServerWithLogger(logger))
	if err != nil {
		ss.Close()
		return nil, err
	}
	return ss, err
}
