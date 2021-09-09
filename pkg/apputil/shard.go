package apputil

import "context"

type ShardExecutor interface {
	Add(ctx context.Context, id string) error
	Drop(ctx context.Context, id string) error
}
