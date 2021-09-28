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

package smserver

import (
	"context"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Server struct {
	// 管理sm server内部的goroutine
	cancel context.CancelFunc

	c  *apputil.Container
	ss *apputil.ShardServer
	sc *smContainer

	lg *zap.Logger
}

type serverOptions struct {
	// id是当前容器/进程的唯一标记，不能变化，用于做container和shard的映射关系
	id string

	// 业务app所在的服务注册发现系统的唯一标记，是业务的别名
	service string

	// etcd集群的配置
	endpoints []string

	// 监听端口: 提供管理职能，add、drop
	addr string
}

type ServerOption func(options *serverOptions)

func WithId(v string) ServerOption {
	return func(options *serverOptions) {
		options.id = v
	}
}

func WithService(v string) ServerOption {
	return func(options *serverOptions) {
		options.service = v
	}
}

func WithEndpoints(v []string) ServerOption {
	return func(options *serverOptions) {
		options.endpoints = v
	}
}

func WithAddr(v string) ServerOption {
	return func(options *serverOptions) {
		options.addr = v
	}
}

func NewServer(fn ...ServerOption) (*Server, error) {
	ops := serverOptions{}
	for _, f := range fn {
		f(&ops)
	}

	if ops.id == "" {
		return nil, errors.New("id err")
	}
	if ops.service == "" {
		return nil, errors.New("service err")
	}
	if ops.addr == "" {
		return nil, errors.New("addr err")
	}
	if len(ops.endpoints) == 0 {
		return nil, errors.New("endpoints err")
	}

	ctx, cancel := context.WithCancel(context.Background())
	logger, _ := zap.NewProduction()
	srv := Server{cancel: cancel, lg: logger}

	c, err := apputil.NewContainer(
		apputil.ContainerWithContext(ctx),
		apputil.ContainerWithService(ops.service),
		apputil.ContainerWithId(ops.id),
		apputil.ContainerWithEndpoints(ops.endpoints),
		apputil.ContainerWithLogger(logger))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	srv.c = c

	sc, err := newContainer(ctx, logger, ops.id, ops.service, c)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	srv.sc = sc

	apiSrv := shardServer{sc, logger}
	routeAndHandler := make(map[string]func(c *gin.Context))
	routeAndHandler["/sm/server/add-spec"] = apiSrv.GinAddSpec
	routeAndHandler["/sm/server/add-shard"] = apiSrv.GinAddShard
	ss, err := apputil.NewShardServer(
		apputil.ShardServerWithAddr(ops.addr),
		apputil.ShardServerWithContext(ctx),
		apputil.ShardServerWithContainer(c),
		apputil.ShardServerWithApiHandler(routeAndHandler),
		apputil.ShardServerWithShardImplementation(sc),
		apputil.ShardServerWithLogger(logger))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	srv.ss = ss

	go func() {
		defer srv.Close()

		// 关注apputil中的Container和ShardServer关闭，sm服务停下来
		select {
		case <-c.Done():
			logger.Info("container done")
		case <-ss.Done():
			logger.Info("shard server done")
		}
	}()

	return &srv, nil
}

func (s *Server) Close() {
	defer s.lg.Sync()

	if s.cancel != nil {
		s.cancel()
	}
}
