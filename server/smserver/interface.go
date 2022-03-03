package smserver

import (
	"io"

	"github.com/entertainment-venue/sm/pkg/apputil"
)

// ShardWrapper 4 unit test
type ShardWrapper interface {
	NewShard(c *smContainer, spec *apputil.ShardSpec) (Shard, error)
}

// Shard 4 unit test
type Shard interface {
	io.Closer

	Spec() *apputil.ShardSpec
	Load() string

	// 下面是SM的Shard特定的
	SetMaxShardCount(maxShardCount int)
	SetMaxRecoveryTime(maxRecoveryTime int)
}
