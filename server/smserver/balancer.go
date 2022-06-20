package smserver

import (
	"fmt"
	"strings"
)

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

type balancerGroupManager struct {
	balancerGroup map[string]*balancerGroup
}

func newBalanceWorkerGroupManager() *balancerGroupManager {
	return &balancerGroupManager{
		balancerGroup: make(map[string]*balancerGroup),
	}
}

func (b *balancerGroupManager) addShard(shardId, containerId, group, workerGroup string) {
	key := generateBalanceGroupKey(group, workerGroup)
	bg := b.balancerGroup[key]
	if bg == nil {
		b.balancerGroup[key] = newBalanceGroup()
	}
	b.balancerGroup[key].fixShardIdAndManualContainerId[shardId] = containerId
}

func (b *balancerGroupManager) addHbShard(shardId, containerId, group, workerGroup string) {
	key := generateBalanceGroupKey(group, workerGroup)
	bg := b.balancerGroup[key]
	if bg == nil {
		b.balancerGroup[key] = newBalanceGroup()
	}
	b.balancerGroup[key].hbShardIdAndContainerId[shardId] = containerId
}

var splitMark = "$$@@##"

func generateBalanceGroupKey(group, workerGroup string) string {
	return fmt.Sprintf("%s%s%s", group, splitMark, workerGroup)
}

func getGroupAndWorkerGroupByKey(key string) (string, string) {
	arr := strings.Split(key, splitMark)
	if len(arr) == 2 {
		return arr[0], arr[1]
	}
	return "", ""
}
