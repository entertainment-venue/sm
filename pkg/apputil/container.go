package apputil

import (
	"context"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/entertainment-venue/borderland/pkg/etcdutil"
	"github.com/pkg/errors"
)

// 1 上报container的load信息，保证container的liveness，才能够参与shard的分配
// 2 与sm交互，下发add和drop给到Shard
type Container struct {
	Client *etcdutil.EtcdClient

	Session *concurrency.Session
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

func NewContainer(ctx context.Context, opts ...ContainerOption) (*Container, error) {
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

	client, err := etcdutil.NewEtcdClient(ops.endpoints)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	container := Container{Client: client}
	container.Session, err = concurrency.NewSession(container.Client.Client, concurrency.WithTTL(5))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	// 上报系统负载，提供container liveness的标记
	ReportSysLoad(&container, EtcdPathAppContainerIdHb(ops.service, ops.id))

	return &container, nil
}
