package smserver

type balancer struct {
	bcs map[string]*balancerContainer
}

type balancerContainer struct {
	// id container标识
	id string

	// shards shard => nothing
	shards map[string]*balancerShard
}

type balancerShard struct {
	// id shard标识
	id string

	// isManual 是否是制定container的
	isManual bool
}

func (b *balancer) put(containerId, shardId string, isManual bool) {
	b.addContainer(containerId)
	b.bcs[containerId].shards[shardId] = &balancerShard{
		id:       shardId,
		isManual: isManual,
	}
}

func (b *balancer) forEach(visitor func(bc *balancerContainer)) {
	for _, bc := range b.bcs {
		visitor(bc)
	}
}

func (b *balancer) addContainer(containerId string) {
	cs := b.bcs[containerId]
	if cs == nil {
		b.bcs[containerId] = &balancerContainer{
			id:     containerId,
			shards: make(map[string]*balancerShard),
		}
	}
}
