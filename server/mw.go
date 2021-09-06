package server

import (
	"context"

	"github.com/coreos/etcd/clientv3"
)

// 抽离出的目的是发现leader和普通的sm shard都具备相似的能力，代码基本一致
type MaintenanceWorker interface {
	Starter
	Closer

	// 监控sm集群本身以及各接入业务app的shard负载
	ShardLoadLoop()

	// 负载的另一个维度，container，相比shard本身的load，在初期更容易理解，也更容易构建
	ContainerLoadLoop()

	// 监控sm集群本身以及各接入业务app的shard分配是否合理
	ShardAllocateLoop()
}

// 管理某个sm app的shard
type maintenanceWorker struct {
	admin

	ctr *container
}

func newMaintenanceWorker(ctr *container, service string) MaintenanceWorker {
	var mw maintenanceWorker
	mw.ctx, mw.cancel = context.WithCancel(context.Background())
	mw.ctr = ctr
	mw.service = service
	return &mw
}

func (w *maintenanceWorker) Start() {
	w.wg.Add(3)
	go w.ShardAllocateLoop()
	go w.ShardLoadLoop()
	go w.ContainerLoadLoop()
}

func (w *maintenanceWorker) Close() {
	w.cancel()
	w.wg.Wait()
	Logger.Printf("maintenanceWorker for service %s stopped", w.ctr.service)
}

func (w *maintenanceWorker) ShardAllocateLoop() {
	tickerLoop(
		w.ctx,
		defaultShardLoopInterval,
		"ShardAllocateLoop exit",
		func(ctx context.Context) error {
			return allocateChecker(ctx, w.ctr.ew, w.service)
		},
		&w.wg,
	)
}

func (w *maintenanceWorker) ShardLoadLoop() {
	watchLoop(
		w.ctx,
		w.ctr.ew,
		w.ctr.ew.nodeAppShardHb(w.service),
		"ShardLoadLoop exit",
		func(ctx context.Context, ev *clientv3.Event) error {
			return shardLoadChecker(ctx, w.service, w.ctr.eq, ev)
		},
		&w.wg,
	)
}

func (w *maintenanceWorker) ContainerLoadLoop() {
	defer w.wg.Done()
	// TODO 监控业务app机器container负载

	watchLoop(
		w.ctx,
		w.ctr.ew,
		w.ctr.ew.nodeAppContainerHb(w.service),
		"ContainerLoadLoop exit",
		func(ctx context.Context, ev *clientv3.Event) error {
			return containerLoadChecker(ctx, w.service, w.ctr.eq, ev)
		},
		&w.wg,
	)

}
