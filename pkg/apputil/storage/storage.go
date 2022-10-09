package storage

import (
	"encoding/json"
	"math"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type StorageType int

const (
	Boltdb StorageType = iota
	Etcd
)

var (
	NoLease = &Lease{
		ID: clientv3.NoLease,
	}
)

type Lease struct {
	// ID rb在bridge和guard两个阶段grant的lease存储在这里，
	// 没有采用存在BridgeLeaseID和GuardLeaseID两个属性的设计，这种设计会导致逻辑使用ID的时候要有选择，没有侧重，随时需要了解所在rb的子阶段
	ID clientv3.LeaseID `json:"id"`

	// Expire 过期时间点，ID本身不是单调递增，LeaseID = memberID + timestamp + counter，如果etcd集群有变动，leaseID可能会减小，
	// leaseID 设计目的是全局唯一即可。
	// 1 leaseID的ttl在sm中如果要缩短，需要停集群超过历史最大leaseID ttl，回收所有历史leaseID，增大leaseID的ttl没有问题
	// 2 带有更新时间点的leaseID，会导致shard被放弃掉
	Expire int64 `json:"expire"`
}

func (l *Lease) EqualTo(lease *Lease) bool {
	return l.ID == lease.ID
}

func (l *Lease) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

func (l *Lease) IsExpired() bool {
	// shardKeeper 提前2s过期，容忍一定范围的机器时钟问题
	// 1 server时间快，是存在问题的，server可能会把该分片分配给别的client
	// 2 server时间慢，client先过期，shard会异常停止，倒是频繁rb
	// 3 2s这个是经验值，机器之间你延迟2秒以上，op接入修复
	return time.Now().Unix() >= (l.Expire + 2)
}

type ShardSpec struct {
	// Id 方法传递的时候可以内容可以自识别，否则，添加分片相关的方法的生命一般是下面的样子：
	// newShard(id string, spec *apputil.ShardSpec)
	Id string `json:"id"`

	// Service 标记自己所在服务，不需要去etcd路径中解析，增加spec的描述性质
	Service string `json:"service"`

	// Task service管理的分片任务内容
	Task string `json:"task"`

	UpdateTime int64 `json:"updateTime"`

	// 通过api可以给shard主动分配到某个container
	ManualContainerId string `json:"manualContainerId"`

	// Group 同一个service需要区分不同种类的shard，
	// 这些shard之间不相关的balance到现有container上
	Group string `json:"group"`

	// WorkerGroup shard只能分配到属于WorkerGroup的container上
	WorkerGroup string `json:"workerGroup"`

	// Lease Add时带上guard lease，存储时可能存bridge和guard
	Lease *Lease `json:"lease"`
}

func (ss *ShardSpec) String() string {
	b, _ := json.Marshal(ss)
	return string(b)
}

func (ss *ShardSpec) Validate() error {
	if ss.Service == "" {
		return errors.New("Empty service")
	}
	if ss.UpdateTime <= 0 {
		return errors.New("Err updateTime")
	}
	return nil
}

// ShardKeeperDbValue 存储分片数据和管理信息
type ShardKeeperDbValue struct {
	// Spec 分片基础信息
	Spec *ShardSpec `json:"spec"`

	// Disp 标记是否已经下发成功
	Disp bool `json:"disp"`

	// Drop 软删除，在异步协程中清理
	Drop bool `json:"drop"`
}

func (dv *ShardKeeperDbValue) String() string {
	b, _ := json.Marshal(dv)
	return string(b)
}

func (dv *ShardKeeperDbValue) SoftMigrate(from, to clientv3.LeaseID) bool {
	// 不需要做移动，逻辑幂等的一部分
	if dv.Spec.Lease.ID == to {
		return false
	}

	if dv.Spec.Lease.ID == from {
		dv.Spec.Lease = &Lease{ID: to, Expire: math.MaxInt64 - 30}
		return true
	} else {
		// 异步删除，下发drop指令到app，通过sync goroutine
		dv.Disp = false
		dv.Drop = true
	}
	return false
}

func (dv *ShardKeeperDbValue) NeedDrop(exclude bool, leaseID clientv3.LeaseID) bool {
	var needDrop bool
	if exclude {
		//  除leaseID都删除
		if leaseID == clientv3.NoLease || leaseID != dv.Spec.Lease.ID {
			needDrop = true
		}
	} else {
		// 只有leaseID需要被删除
		if leaseID == dv.Spec.Lease.ID {
			needDrop = true
		}
	}
	return needDrop
}

// Storage
// bolt引入的初衷是让shard提交给app和sm的shard分配解耦：
// 1 尽可能提高shard rb的稳定性（成功率）
// 2 提交给app这个环节，具备补偿能力，sync goroutine不断尝试能够让app感知明确（错误日志/报警）
type Storage interface {
	Close() error

	// Add
	// 添加分片
	Add(shard *ShardSpec) error

	// Drop
	// bridge阶段，drop掉本地的命中shard
	Drop(ids []string) error

	// ForEach
	// 遍历所有shard
	ForEach(visitor func(shardID string, dv *ShardKeeperDbValue) error) error

	// MigrateLease
	// 1 从旧的guard lease迁移到bridge lease
	// 2 从bridge lease迁移到新的guard lease
	MigrateLease(from, to clientv3.LeaseID) error

	// DropByLease
	// bridge阶段应对批量删除场景
	DropByLease(exclude bool, leaseID clientv3.LeaseID) error

	// Reset
	// 标记所有shard待下发，shardkeeper启动时使用
	Reset() error

	// Put
	// 区分于Update，允许包含特定于实现的逻辑
	Put(shardID string, dv *ShardKeeperDbValue) error

	// Remove
	// 区分于Delete，语序包含特定于实现的逻辑
	Remove(shardID string) error

	// Update
	// for unittest
	Update(k, v []byte) error
	// Delete
	// for unittest
	Delete(k []byte) error
	// Get
	// for unittest
	Get(k []byte) ([]byte, error)
	// Clear
	// for unittest
	Clear() error
}
