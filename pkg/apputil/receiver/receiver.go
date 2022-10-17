package receiver

import "github.com/entertainment-venue/sm/pkg/apputil/core"

type Receiver interface {
	Start() error
	Shutdown() error

	// Extract 开放方法，用于上层获取http engine，sm使用该方法。扩展场景可以不实现
	Extract() interface{}

	// SetShardPrimitives 这块的设计有点糙，receiver的初始化在container的new方法，new方法之后允许app做route增加，然后，receiver依赖shardkeeper，shardkeeper的初始化需要在run方法，所以导致对于receiver的初始化分布在两个地方
	SetShardPrimitives(_ core.ShardPrimitives)
}
