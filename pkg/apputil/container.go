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

// 1 上报container的load信息，保证container的liveness，才能够参与shard的分配
// 2 与sm交互，下发add和drop给到Shard
type Container struct {
	Client *etcdutil.EtcdClient

	Session *concurrency.Session

	Stopper *GoroutineStopper

	id, service string
	donec       <-chan struct{}

	lg *zap.Logger
}

type containerOptions struct {
	id        string
	service   string
	endpoints []string
	ctx       context.Context
}

type ContainerOption func(options *containerOptions)

func WithId(v string) ContainerOption {
	return func(co *containerOptions) {
		co.id = v
	}
}

func WithService(v string) ContainerOption {
	return func(co *containerOptions) {
		co.service = v
	}
}

func WithEndpoints(v []string) ContainerOption {
	return func(co *containerOptions) {
		co.endpoints = v
	}
}

func WithContext(ctx context.Context) ContainerOption {
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
	if len(ops.endpoints) == 0 {
		return nil, errors.New("endpoints err")
	}

	logger, _ := zap.NewProduction()

	client, err := etcdutil.NewEtcdClient(ops.endpoints, logger)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	s, err := concurrency.NewSession(client.Client, concurrency.WithTTL(5))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	donec := make(chan struct{})
	container := Container{Client: client, Session: s, id: ops.id, service: ops.service, donec: donec, lg: logger}

	go func() {
		defer close(donec)
		defer container.Close()
		for range container.Session.Done() {

		}
		container.lg.Info("session closed",
			zap.String("service", ops.service),
			zap.String("id", ops.id),
		)
	}()

	// 上报系统负载，提供container liveness的标记
	container.Stopper.Wrap(
		func(ctx context.Context) {
			TickerLoop(ctx, logger, 3*time.Second, "Container upload exit", container.UploadSysLoad)
		})

	return &container, nil
}

func (c *Container) Close() {
	if c.Stopper != nil {
		c.Stopper.Close()
	}
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
