package smserver

import (
	"io"

	"github.com/entertainment-venue/sm/pkg/apputil"
)

// ShardWrapper 4 unit test
type ShardWrapper interface {
	NewShard(c *smContainer, spec *apputil.ShardSpec) (Shard, error)
}

type smShardWrapper struct {
	ss *smShard
}

func (s *smShardWrapper) NewShard(c *smContainer, spec *apputil.ShardSpec) (Shard, error) {
	return newShard(c, spec)
}

// Shard 4 unit test
type Shard interface {
	io.Closer

	Spec() *apputil.ShardSpec
	Load() string
	Worker() *Worker
}
