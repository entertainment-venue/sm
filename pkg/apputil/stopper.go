package apputil

import (
	"context"
	"sync"
)

// 提出container和shard的公共属性
// 抽象数据结构，也会引入数据结构之间的耦合
type GoroutineStopper struct {
	once sync.Once

	// https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
	// goroutineStopper close
	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

type StopperFunc func(ctx context.Context) error

func (stopper *GoroutineStopper) Run(ctx context.Context, fn StopperFunc) {
	stopper.once.Do(func() {
		stopper.ctx, stopper.cancel = context.WithCancel(ctx)
	})

	stopper.wg.Add(1)
	go func(fn StopperFunc, ctx context.Context) {
		defer stopper.wg.Done()

		if err := fn(ctx); err != nil {
			panic(err)
		}
	}(fn, stopper.ctx)
}

func (stopper *GoroutineStopper) Close() {
	if stopper.cancel != nil {
		stopper.cancel()
	}
	stopper.wg.Wait()
}
