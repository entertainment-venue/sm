// Copyright 2021 The entertainment-venue Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Client struct {
	stopper     *apputil.GoroutineStopper
	lg          *zap.Logger
	container   *apputil.Container
	shardServer *apputil.ShardServer
	opts        *clientOptions
}

type clientOptions struct {
	g           *gin.Engine
	service     string
	containerId string
	etcdPrefix  string
	etcdAddr    []string
	v           apputil.ShardInterface
}

var defaultSmPrefix = "/sm"

type ClientOption func(options *clientOptions)

func ClientWithRouter(g *gin.Engine) ClientOption {
	return func(so *clientOptions) {
		so.g = g
	}
}

func ClientWithService(service string) ClientOption {
	return func(so *clientOptions) {
		so.service = service
	}
}

func ClientWithContainerId(containerId string) ClientOption {
	return func(so *clientOptions) {
		so.containerId = containerId
	}
}

func ClientWithEtcdPrefix(etcdPrefix string) ClientOption {
	return func(so *clientOptions) {
		so.etcdPrefix = etcdPrefix
	}
}

func ClientWithEtcdAddr(etcdAddr []string) ClientOption {
	return func(so *clientOptions) {
		so.etcdAddr = etcdAddr
	}
}

func ClientWithImplementation(v apputil.ShardInterface) ClientOption {
	return func(so *clientOptions) {
		so.v = v
	}
}

func NewClient(opts ...ClientOption) (*Client, error) {
	ops := &clientOptions{}
	for _, opt := range opts {
		opt(ops)
	}
	if ops.g == nil {
		return nil, errors.New("gin router empty")
	}
	if ops.service == "" {
		return nil, errors.New("service empty")
	}
	if ops.containerId == "" {
		return nil, errors.New("containerId empty")
	}
	if ops.etcdPrefix == "" {
		ops.etcdPrefix = defaultSmPrefix
	}
	if ops.etcdAddr == nil {
		return nil, errors.New("etcdAddr empty")
	}
	if ops.v == nil {
		return nil, errors.New("impl empty")
	}

	lg, err := zap.NewProduction()
	if err != nil {
		return nil, errors.Wrap(err, "new zap logger failed")
	}

	c := &Client{
		stopper: &apputil.GoroutineStopper{},
		lg:      lg,
		opts:    ops,
	}
	if err := c.newServer(); err != nil {
		lg.Error("new server failed",
			zap.String("service", ops.service),
			zap.String("err", err.Error()),
		)
		return nil, err
	}

	c.stopper.Wrap(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				c.lg.Info("client exit")
				return
			case <-c.shardServer.Done():
				lg.Info("session done, try again")
				if err := c.newServer(); err != nil {
					lg.Error("new server failed",
						zap.String("service", ops.service),
						zap.String("err", err.Error()),
					)
					time.Sleep(3 * time.Second)
				}
			}
		}
	})
	return c, nil
}

func (c *Client) newServer() error {
	container, err := apputil.NewContainer(
		apputil.ContainerWithService(c.opts.service),
		apputil.ContainerWithId(c.opts.containerId),
		apputil.ContainerWithEndpoints(c.opts.etcdAddr),
		apputil.ContainerWithLogger(c.lg))
	if err != nil {
		return errors.Wrap(err, "new container failed")
	}

	shardServer, err := apputil.NewShardServer(
		apputil.ShardServerWithEtcdPrefix(c.opts.etcdPrefix),
		apputil.ShardServerWithRouter(c.opts.g),
		apputil.ShardServerWithContainer(container),
		apputil.ShardServerWithShardImplementation(c.opts.v),
		apputil.ShardServerWithLogger(c.lg))
	if err != nil {
		container.Close()
		return errors.Wrap(err, "new shard server failed")
	}
	c.container = container
	c.shardServer = shardServer
	return nil
}

func (c *Client) Close() {
	if c.stopper != nil {
		c.stopper.Close()
	}
	c.container.Close()
	c.shardServer.Close()
}
