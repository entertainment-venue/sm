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

// balancerGroup 同一个container支持在shard维度支持分组，分开balance
type balancerGroup struct {
	// fixShardIdAndManualContainerId shard配置
	fixShardIdAndManualContainerId ArmorMap

	// hbShardIdAndContainerId shard心跳
	hbShardIdAndContainerId ArmorMap
}

func newBalanceGroup() *balancerGroup {
	return &balancerGroup{
		fixShardIdAndManualContainerId: make(ArmorMap),
		hbShardIdAndContainerId:        make(ArmorMap),
	}
}

type balancerWorkerGroup struct {
	workerGroup map[string]*balancerGroup
}

func newBalanceWorkerGroup() *balancerWorkerGroup {
	return &balancerWorkerGroup{
		workerGroup: make(map[string]*balancerGroup),
	}
}

func (b *balancerWorkerGroup) addShard(shardId string, containerId string, workerGroup string) {
	bg := b.workerGroup[workerGroup]
	if bg == nil {
		b.workerGroup[workerGroup] = newBalanceGroup()
	}
	b.workerGroup[workerGroup].fixShardIdAndManualContainerId[shardId] = containerId
}

func (b *balancerWorkerGroup) addHbShard(shardId string, containerId string, workerGroup string) {
	bg := b.workerGroup[workerGroup]
	if bg == nil {
		b.workerGroup[workerGroup] = newBalanceGroup()
	}
	b.workerGroup[workerGroup].hbShardIdAndContainerId[shardId] = containerId
}
