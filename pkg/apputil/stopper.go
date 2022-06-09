// Copyright 2021 The entertainment-venue Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apputil

import (
	"context"
	"sync"
)

// GoroutineStopper 提出container和shard的公共属性
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
	stopper.once.Do(
		func() {
			// 不需要外部ctx
			stopper.ctx, stopper.cancel = context.WithCancel(context.TODO())
		},
	)

	stopper.wg.Add(1)
	go func(fn StopableFunc, ctx context.Context) {
		defer stopper.wg.Done()

		fn(ctx)
	}(fn, stopper.ctx)
}

func (stopper *GoroutineStopper) Close() {
	if stopper.cancel != nil {
		stopper.cancel()

		// bugfix: stopper调用Close之后，再次调用Wrap，这时的ctx和cancel不合法，会导致goroutine立即退出，在Close之后需要重新初始化
		stopper.ctx, stopper.cancel = context.WithCancel(context.TODO())
	}
	stopper.wg.Wait()
}
