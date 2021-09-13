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

type StopableFunc func(ctx context.Context)

func (stopper *GoroutineStopper) Wrap(fn StopableFunc) {
	stopper.once.Do(func() {
		// 不需要外部ctx
		stopper.ctx, stopper.cancel = context.WithCancel(context.TODO())
	})

	stopper.wg.Add(1)
	go func(fn StopableFunc, ctx context.Context) {
		defer stopper.wg.Done()

		fn(ctx)
	}(fn, stopper.ctx)
}

func (stopper *GoroutineStopper) Close() {
	if stopper.cancel != nil {
		stopper.cancel()
	}
	stopper.wg.Wait()
}
