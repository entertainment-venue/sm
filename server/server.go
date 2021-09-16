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

package server

import (
	"context"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type serverOptions struct {
	// id是当前容器/进程的唯一标记，不能变化，用于做container和shard的映射关系
	id string

	// 业务app所在的服务注册发现系统的唯一标记，是业务的别名
	service string

	// etcd集群的配置
	endpoints []string

	// 监听端口: 提供管理职能，add、drop
	addr string

	ctx context.Context
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

func WithContext(v context.Context) ServerOption {
	return func(options *serverOptions) {
		options.ctx = v
	}
}

func Run(fn ...ServerOption) error {
	ops := serverOptions{}
	for _, f := range fn {
		f(&ops)
	}

	if ops.id == "" || ops.service == "" || ops.addr == "" || len(ops.endpoints) == 0 {
		return errors.Wrap(errParam, "")
	}

	logger, _ := zap.NewProduction()

	cc, err := apputil.NewContainer(
		apputil.ContainerWithContext(ops.ctx),
		apputil.ContainerWithService(ops.service),
		apputil.ContainerWithId(ops.id),
		apputil.ContainerWithEndpoints(ops.endpoints),
		apputil.ContainerWithLogger(logger))
	if err != nil {
		return errors.Wrap(err, "")
	}

	sc, err := launchServerContainer(ops.ctx, logger, ops.id, ops.service)
	if err != nil {
		return errors.Wrap(err, "")
	}

	go func() {
		defer logger.Sync()
		defer sc.Close()
		for range cc.Done() {

		}
	}()

	ss := shardServer{sc, logger}
	routeAndHandler := make(map[string]func(c *gin.Context))
	routeAndHandler["/sm/server/add-spec"] = ss.GinAddSpec
	routeAndHandler["/sm/server/add-shard"] = ss.GinAddShard

	if err := apputil.NewShardServer(
		apputil.ShardServerWithAddr(ops.addr),
		apputil.ShardServerWithContext(ops.ctx),
		apputil.ShardServerWithContainer(cc),
		apputil.ShardServerWithApiHandler(routeAndHandler),
		apputil.ShardServerWithShardImplementation(sc),
		apputil.ShardServerWithLogger(logger)); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
