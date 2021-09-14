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
	"log"
	"os"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

// StdLogger is used to log error messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var Logger StdLogger = log.New(os.Stdout, "[BORDERLAND] ", log.LstdFlags|log.Lshortfile)

type Starter interface {
	Start()
}

type Closer interface {
	Close()
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
	opts := serverOptions{}
	for _, f := range fn {
		f(&opts)
	}

	if opts.id == "" || opts.service == "" || opts.addr == "" || len(opts.endpoints) == 0 {
		return errors.Wrap(errParam, "")
	}

	cc, err := apputil.NewContainer(
		apputil.WithContext(opts.ctx),
		apputil.WithService(opts.service),
		apputil.WithId(opts.id),
		apputil.WithEndpoints(opts.endpoints))
	if err != nil {
		return errors.Wrap(err, "")
	}

	sc, err := newServerContainer(opts.ctx, opts.id, opts.service, opts.endpoints)
	if err != nil {
		return errors.Wrap(err, "")
	}

	go func() {
		defer sc.Close()
		for range cc.Done() {

		}
	}()

	ss := shardServer{sc}
	routeAndHandler := make(map[string]func(c *gin.Context))
	routeAndHandler["/sm/admin/add-spec"] = ss.GinAppAddSpec
	routeAndHandler["/sm/admin/add-shard"] = ss.GinAppAddShard

	if err := apputil.NewShardServer(
		apputil.ShardServerWithAddr(opts.addr),
		apputil.ShardServerWithContext(opts.ctx),
		apputil.ShardServerWithContainer(cc),
		apputil.ShardServerWithApiHandler(routeAndHandler),
		apputil.ShardServerWithShardImplementation(sc)); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
