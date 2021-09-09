package apputil

import (
	"context"

	"github.com/pkg/errors"
)

// 1 上报container的load信息，保证container的liveness，才能够参与shard的分配
// 2 与sm交互，下发add和drop给到Shard
type Container struct{}

type containerOptions struct {
	id        string
	endpoints []string
	ctx       context.Context
}

type ContainerOption func(options *containerOptions)

func WithId(v string) ContainerOption {
	return func(co *containerOptions) {
		co.id = v
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

func NewContainer(_ context.Context, opts ...ContainerOption) error {
	ops := &containerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	if ops.id == "" {
		return errors.New("id err")
	}
	if len(ops.endpoints) == 0 {
		return errors.New("endpoints err")
	}

	return nil
}
