package smserver

import (
	"io"

	"github.com/entertainment-venue/sm/pkg/apputil/storage"
)

// ShardWrapper 4 unit test
type ShardWrapper interface {
	NewShard(c *smContainer, spec *storage.ShardSpec) (Shard, error)
}

// Shard 4 unit test
type Shard interface {
	io.Closer

	Spec() *storage.ShardSpec

	// SetMaxShardCount 和 SetMaxRecoveryTime 下面是SM的Shard特定的
	SetMaxShardCount(maxShardCount int)
	SetMaxRecoveryTime(maxRecoveryTime int)
}
