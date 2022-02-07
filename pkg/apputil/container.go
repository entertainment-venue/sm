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
	"sync"
	"time"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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

	// donec 可以通知调用方
	donec chan struct{}

	mu sync.Mutex
	// closed 导致 Container 被关闭的事件是异步的，需要做保护
	closed bool
}

type containerOptions struct {
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

	ec, err := etcdutil.NewEtcdClient(ops.endpoints, ops.lg)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
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

	// 通过heartbeat上报数据
	c.stopper.Wrap(
		func(ctx context.Context) {
			TickerLoop(ctx, ops.lg, 3*time.Second, "container stop upload load", c.UploadSysLoad)
		},
	)

	// 1 监控session，关注etcd导致的异常关闭
	// 2 使用donec，关注外部调用Close导致的关闭
	go func() {
		select {
		case <-c.donec:
			// 被动关闭
			c.lg.Info("container: stopper closed",
				zap.String("id", c.Id()),
				zap.String("service", c.Service()),
			)
		case <-c.Session.Done():
			// 主动关闭
			c.close()

			c.lg.Info("container: session closed",
				zap.String("id", c.Id()),
				zap.String("service", c.Service()),
			)
		}
	}()

	return &c, nil
}

func (c *Container) Close() {
	c.close()

	c.lg.Info("container: closed",
		zap.String("id", c.Id()),
		zap.String("service", c.Service()),
	)
}

func (c *Container) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if c.stopper != nil {
		c.stopper.Close()
	}
	close(c.donec)
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

type ContainerHeartbeat struct {
	VirtualMemoryStat  *mem.VirtualMemoryStat `json:"virtualMemoryStat"`
	CPUUsedPercent     float64                `json:"cpuUsedPercent"`
	DiskIOCountersStat []*disk.IOCountersStat `json:"diskIOCountersStat"`
	NetIOCountersStat  *net.IOCountersStat    `json:"netIOCountersStat"`

	// Timestamp sm中用于计算container删除事件的等待时间
	Timestamp int64 `json:"timestamp"`
}

func (l *ContainerHeartbeat) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

func (c *Container) UploadSysLoad(ctx context.Context) error {
	ld := ContainerHeartbeat{Timestamp: time.Now().Unix()}

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
