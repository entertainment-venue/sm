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
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/entertainment-venue/sm/pkg/logutil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Client struct {
	stopper   *apputil.GoroutineStopper
	lg        *zap.Logger
	container *apputil.Container
	opts      *clientOptions
}

type clientOptions struct {
	g           *gin.Engine
	service     string
	containerId string
	etcdPrefix  string
	etcdAddr    []string
	v           apputil.ShardInterface
	// shard.db存储路径，默认是当前路径
	shardDir string
	// 日志存储路径
	logPath string
	// 日志输出console格式
	logConsole bool
}

var defaultClientOptions = &clientOptions{
	etcdPrefix: "/sm",
	logPath:    "./log",
	logConsole: false,
}

type ClientOption func(options *clientOptions)

func ClientWithRouter(g *gin.Engine) ClientOption {
	return func(co *clientOptions) {
		co.g = g
	}
}

func ClientWithService(service string) ClientOption {
	return func(co *clientOptions) {
		co.service = service
	}
}

func ClientWithContainerId(containerId string) ClientOption {
	return func(co *clientOptions) {
		co.containerId = containerId
	}
}

func ClientWithEtcdPrefix(etcdPrefix string) ClientOption {
	return func(co *clientOptions) {
		co.etcdPrefix = etcdPrefix
	}
}

func ClientWithEtcdAddr(etcdAddr []string) ClientOption {
	return func(co *clientOptions) {
		co.etcdAddr = etcdAddr
	}
}

func ClientWithImplementation(v apputil.ShardInterface) ClientOption {
	return func(co *clientOptions) {
		co.v = v
	}
}

func ClientWithShardDir(v string) ClientOption {
	return func(co *clientOptions) {
		co.shardDir = v
	}
}

func ClientWithLogPath(v string) ClientOption {
	return func(co *clientOptions) {
		co.logPath = v
	}
}

func ClientWithLogConsole(v bool) ClientOption {
	return func(co *clientOptions) {
		co.logConsole = v
	}
}

func NewClient(opts ...ClientOption) (*Client, error) {
	ops := defaultClientOptions
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
	if ops.etcdAddr == nil {
		return nil, errors.New("etcdAddr empty")
	}
	if ops.v == nil {
		return nil, errors.New("impl empty")
	}

	lg, err := logutil.NewLogger(logutil.WithPath(fmt.Sprintf("%s/%s", ops.logPath, "sm.log")), logutil.WithEncodingConsole(ops.logConsole))
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

	// 是否需要跳过给router挂接口
	var skip bool
	for _, route := range ops.g.Routes() {
		if strings.HasPrefix(route.Path, "/sm/admin") {
			skip = true
			break
		}
	}

	if !skip {
		ssg := ops.g.Group("/sm/admin")
		{
			ssg.POST("/add-shard", c.AddShard)
			ssg.POST("/drop-shard", c.DropShard)
		}
	}

	c.stopper.Wrap(
		func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					c.lg.Info("client exit")
					return
				case <-c.container.Done():
					lg.Info("session done, try again")
					for {
						select {
						case <-ctx.Done():
							c.lg.Info("client exit")
							return
						default:
						}
						if err := c.newServer(); err == nil {
							lg.Info("restart success")
							break
						}
						lg.Error("new server failed",
							zap.String("service", ops.service),
							zap.Error(err),
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
		apputil.WithService(c.opts.service),
		apputil.WithId(c.opts.containerId),
		apputil.WithEndpoints(c.opts.etcdAddr),
		apputil.WithEtcdPrefix(c.opts.etcdPrefix),
		apputil.WithLogger(c.lg),
		apputil.WithShardImplementation(c.opts.v),
		apputil.WithShardDir(c.opts.shardDir))
	if err != nil {
		return errors.Wrap(err, "new container failed")
	}
	// container.Run()里面的协程调用了c.container的方法，所以必须先c.container = container，才能Run,否则可能会panic
	c.container = container
	if err := c.container.Run(); err != nil {
		if container != nil {
			container.Close()
		}
		return errors.Wrap(err, "")
	}
	return nil
}

func (c *Client) AddShard(g *gin.Context) {
	if c.container != nil {
		c.container.AddShard(g)
	} else {
		c.lg.Error("Container is nil or not initialized")
		g.JSON(http.StatusInternalServerError, gin.H{"error": errors.New("Container is nil or not initialized")})
	}
}

func (c *Client) DropShard(g *gin.Context) {
	if c.container != nil {
		c.container.DropShard(g)
	} else {
		c.lg.Error("Container is nil or not initialized")
		g.JSON(http.StatusInternalServerError, gin.H{"error": errors.New("Container is nil or not initialized")})
	}
}

// Close Client关闭以后，gin.Router不能重用。
func (c *Client) Close() {
	if c.stopper != nil {
		c.stopper.Close()
	}
	c.container.Close()
}
