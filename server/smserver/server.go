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
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	_ "github.com/entertainment-venue/sm/server/docs"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	swaggerfiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"
)

type Server struct {
	c   *apputil.Container
	ss  *apputil.ShardServer
	smc *smContainer

	opts    *serverOptions
	stopper *apputil.GoroutineStopper
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
	if ops.ctx == nil {
		ops.ctx = context.Background()
	}
	apputil.InitEtcdPrefix(ops.etcdPrefix)

	srv := Server{opts: &ops, stopper: &apputil.GoroutineStopper{}}
	if err := srv.run(); err != nil {
		return nil, err
	}

	srv.stopper.Wrap(
		func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					srv.opts.lg.Info("client exit")
					return
				case <-srv.ss.Done():
					srv.smc.Close()
					// 关注apputil中的ShardServer关闭，尝试重新连接
					srv.opts.lg.Info("session done, try again")
					if err := srv.run(); err != nil {
						srv.opts.lg.Error("new server failed",
							zap.String("service", ops.service),
							zap.String("err", err.Error()),
						)
						time.Sleep(3 * time.Second)
					}
				}
			}
		})

	go func() {
		defer srv.Close()

		// 允许外部通过ctx，控制sm服务停下来
		select {
		case <-srv.opts.ctx.Done():
			ops.lg.Info("main exit server")
		}
	}()

	return &srv, nil
}

func (s *Server) run() error {
	c, err := apputil.NewContainer(
		apputil.ContainerWithService(s.opts.service),
		apputil.ContainerWithId(s.opts.id),
		apputil.ContainerWithEndpoints(s.opts.endpoints),
		apputil.ContainerWithLogger(s.opts.lg))
	if err != nil {
		return errors.Wrap(err, "new container failed")
	}
	s.c = c

	sc, err := newSMContainer(s.opts.lg, s.opts.id, s.opts.service, c)
	if err != nil {
		c.Close()
		return errors.Wrap(err, "new SM container failed")
	}
	s.smc = sc

	apiSrv := shardServer{sc, s.opts.lg}
	routeAndHandler := make(map[string]func(c *gin.Context))
	routeAndHandler["/sm/server/add-spec"] = apiSrv.GinAddSpec
	routeAndHandler["/sm/server/del-spec"] = apiSrv.GinDelSpec
	routeAndHandler["/sm/server/get-spec"] = apiSrv.GinGetSpec
	routeAndHandler["/sm/server/update-spec"] = apiSrv.GinUpdateSpec
	routeAndHandler["/sm/server/add-shard"] = apiSrv.GinAddShard
	routeAndHandler["/sm/server/del-shard"] = apiSrv.GinDelShard
	routeAndHandler["/sm/server/get-shard"] = apiSrv.GinGetShard
	routeAndHandler["/swagger/*any"] = ginSwagger.WrapHandler(swaggerfiles.Handler)
	ss, err := apputil.NewShardServer(
		apputil.ShardServerWithAddr(s.opts.addr),
		apputil.ShardServerWithContainer(c),
		apputil.ShardServerWithApiHandler(routeAndHandler),
		apputil.ShardServerWithShardImplementation(sc),
		apputil.ShardServerWithLogger(s.opts.lg),
		apputil.ShardServerWithEtcdPrefix(s.opts.etcdPrefix))
	if err != nil {
		c.Close()
		sc.Close()
		return errors.Wrap(err, "new shard server failed")
	}
	s.ss = ss
	return nil
}

func (s *Server) Close() {
	defer s.opts.lg.Sync()
	if s.stopper != nil {
		s.stopper.Close()
	}
	s.c.Close()
	s.smc.Close()
	s.ss.Close()
	// 通知上游，保留像上游传递error的能力
	close(s.opts.stopped)
}
