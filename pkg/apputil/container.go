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

package apputil

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"go.uber.org/zap"
)

// Container 1 上报container的load信息，保证container的liveness，才能够参与shard的分配
// 2 与sm交互，下发add和drop给到Shard
type Container struct {
	Client  *etcdutil.EtcdClient
	Session *concurrency.Session
	stopper *GoroutineStopper

	id      string
	service string
	lg      *zap.Logger

	// 可以通知调用方Container结束
	donec chan struct{}
}

type containerOptions struct {
	ctx       context.Context
	endpoints []string

	// 数据传递
	id      string
	service string
	lg      *zap.Logger
}

type ContainerOption func(options *containerOptions)

func ContainerWithId(v string) ContainerOption {
	return func(co *containerOptions) {
		co.id = v
	}
}

func ContainerWithService(v string) ContainerOption {
	return func(co *containerOptions) {
		co.service = v
	}
}

func ContainerWithEndpoints(v []string) ContainerOption {
	return func(co *containerOptions) {
		co.endpoints = v
	}
}

func ContainerWithLogger(lg *zap.Logger) ContainerOption {
	return func(co *containerOptions) {
		co.lg = lg
	}
}

func ContainerWithContext(ctx context.Context) ContainerOption {
	return func(co *containerOptions) {
		co.ctx = ctx
	}
}

func NewContainer(opts ...ContainerOption) (*Container, error) {
	ops := &containerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	if ops.id == "" {
		return nil, errors.New("id err")
	}
	if ops.service == "" {
		return nil, errors.New("service err")
	}
	if len(ops.endpoints) == 0 {
		return nil, errors.New("endpoints err")
	}
	if ops.lg == nil {
		return nil, errors.New("lg err")
	}
	if ops.ctx == nil {
		return nil, errors.New("ctx err")
	}

	ec, err := etcdutil.NewEtcdClient(ops.endpoints, ops.lg)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	// FIXME etcd不启动，这里会hang住，127.0.0.1.2379
	s, err := concurrency.NewSession(ec.Client, concurrency.WithTTL(5))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	ops.lg.Info("session opened",
		zap.String("id", ops.id),
		zap.String("service", ops.service),
	)

	c := Container{
		Client:  ec,
		Session: s,
		stopper: &GoroutineStopper{},

		id:      ops.id,
		service: ops.service,
		donec:   make(chan struct{}),
		lg:      ops.lg,
	}

	// 上报系统负载，提供container liveness的标记
	c.stopper.Wrap(
		func(ctx context.Context) {
			TickerLoop(ctx, ops.lg, 3*time.Second, "container stop upload load", c.UploadSysLoad)
		})

	go func() {
		// session关闭后，停掉当前container的工作goroutine
		defer c.Close()

		// 监控session的关闭
		// 需要监控两个方面:
		// 1. 与etcd网络连接问题导致session不稳定
		// 2. 使用apputil的app主动通过ctx关闭
		select {
		case <-c.Session.Done():
			c.lg.Info("session closed",
				zap.String("id", c.Id()),
				zap.String("service", c.Service()),
			)
		case <-ops.ctx.Done():
			c.lg.Info("context done",
				zap.String("id", c.Id()),
				zap.String("service", c.Service()),
			)
		}
	}()

	return &c, nil
}

func (c *Container) Close() {
	defer close(c.donec)

	if c.stopper != nil {
		c.stopper.Close()
	}
	c.lg.Info("container closed",
		zap.String("id", c.Id()),
		zap.String("service", c.Service()),
	)
}

func (c *Container) Done() <-chan struct{} {
	return c.donec
}

func (c *Container) Id() string {
	return c.id
}

func (c *Container) Service() string {
	return c.service
}

type SysLoad struct {
	VirtualMemoryStat  *mem.VirtualMemoryStat `json:"virtualMemoryStat"`
	CPUUsedPercent     float64                `json:"cpuUsedPercent"`
	DiskIOCountersStat []*disk.IOCountersStat `json:"diskIOCountersStat"`
	NetIOCountersStat  *net.IOCountersStat    `json:"netIOCountersStat"`
}

func (l *SysLoad) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

func (c *Container) UploadSysLoad(ctx context.Context) error {
	ld := SysLoad{}

	// 内存使用比率
	vm, err := mem.VirtualMemory()
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.VirtualMemoryStat = vm

	// cpu使用比率
	cp, err := cpu.Percent(0, false)
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.CPUUsedPercent = cp[0]

	// 磁盘io使用比率
	diskIOCounters, err := disk.IOCounters()
	if err != nil {
		return errors.Wrap(err, "")
	}
	for _, v := range diskIOCounters {
		ld.DiskIOCountersStat = append(ld.DiskIOCountersStat, &v)
	}

	// 网路io使用比率
	netIOCounters, err := net.IOCounters(false)
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.NetIOCountersStat = &netIOCounters[0]

	if _, err := c.Client.Put(ctx, EtcdPathAppContainerIdHb(c.service, c.id), ld.String(), clientv3.WithLease(c.Session.Lease())); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}
