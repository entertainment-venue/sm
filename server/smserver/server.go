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

	c   *apputil.Container
	ss  *apputil.ShardServer
	smc *smContainer

	lg      *zap.Logger
	stopped chan error
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

	// 可以接受调用方的主动停止
	ctx context.Context
	// 内部有问题要通知调用方
	stopped chan error

	lg *zap.Logger

	// etcdPrefix 这个路径是etcd中开辟出来给sm使用的，etcd可能是多个组件公用
	// TODO 要有用户名和密码限制
	etcdPrefix string
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

func WithCtx(v context.Context) ServerOption {
	return func(options *serverOptions) {
		options.ctx = v
	}
}

func WithStopped(v chan error) ServerOption {
	return func(options *serverOptions) {
		options.stopped = v
	}
}

func WithLogger(v *zap.Logger) ServerOption {
	return func(options *serverOptions) {
		options.lg = v
	}
}

func WithEtcdPrefix(v string) ServerOption {
	return func(options *serverOptions) {
		options.etcdPrefix = v
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
	if ops.lg == nil {
		return nil, errors.New("logger err")
	}
	apputil.InitEtcdPrefix(ops.etcdPrefix)

	// 允许调用方对server进行控制
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if ops.ctx == nil {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithCancel(ops.ctx)
	}

	logger := ops.lg
	srv := Server{cancel: cancel, lg: logger, stopped: ops.stopped}

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

	sc, err := newSMContainer(ctx, logger, ops.id, ops.service, c)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	srv.smc = sc

	apiSrv := shardServer{sc, logger}
	routeAndHandler := make(map[string]func(c *gin.Context))
	routeAndHandler["/sm/server/add-spec"] = apiSrv.GinAddSpec
	routeAndHandler["/sm/server/add-shard"] = apiSrv.GinAddShard
	routeAndHandler["/sm/server/del-shard"] = apiSrv.GinDelShard
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
		case <-ctx.Done():
			logger.Info("main exit server")
		}
	}()

	return &srv, nil
}

func (s *Server) Close() {
	defer s.lg.Sync()

	if s.cancel != nil {
		s.cancel()
	}

	// 通知上游，保留像上游传递error的能力
	close(s.stopped)
}
