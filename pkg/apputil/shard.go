package apputil

import "context"

type ShardInterface interface {
	Add(ctx context.Context, id string) error
	Drop(ctx context.Context, id string) error
	Load(ctx context.Context, id string) (string, error)
}
