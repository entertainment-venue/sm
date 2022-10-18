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

package commonutil

import (
	"context"
	"time"

	"github.com/entertainment-venue/sm/pkg/logutil"
)

type ErrFunc func(err error)

func LogErrFunc(err error) {
	logutil.SError("ticker loop meet error, error is %+v", err)
}

func TickerLoop(ctx context.Context, errFn ErrFunc, duration time.Duration, fn func(ctx context.Context) error) {
	tickerLoop(ctx, errFn, duration, fn, false)
}

func SequenceTickerLoop(ctx context.Context, errFn ErrFunc, duration time.Duration, exitMsg string, fn func(ctx context.Context) error) {
	tickerLoop(ctx, errFn, duration, fn, true)
}

func tickerLoop(ctx context.Context, errFn ErrFunc, duration time.Duration, fn func(ctx context.Context) error, sequence bool) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		if sequence {
			// 想要周期又不想要因为fn执行慢，导致多个goroutine并行执行，例如：周期检查rb
			if err := fn(ctx); err != nil {
				errFn(err)
			}
		} else {
			// 参考PD的lease.go修正ticker的机制
			// 1 先运行一次loop，算是降低下接入应用的RD在心智压力，让程序更可以预估，防止duration需要等待好久的场景
			// 2 利用goroutine隔离不同的fn，尽可能防止单次goroutine STW或者block，但etcd其实健康的情况，没有心跳，但也有goroutine泄漏的风险
			go func() {
				if err := fn(ctx); err != nil {
					errFn(err)
				}
			}()
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}
