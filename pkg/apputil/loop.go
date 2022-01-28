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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TickerLoop(ctx context.Context, lg *zap.Logger, duration time.Duration, exitMsg string, fn func(ctx context.Context) error) {
	ticker := time.Tick(duration)
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			lg.Info(exitMsg)
			return
		}
		if err := fn(ctx); err != nil {
			lg.Error("fn err", zap.Error(err))
		}
	}
}

func WatchLoop(ctx context.Context, lg *zap.Logger, client *clientv3.Client, node string, exitMsg string, fn func(ctx context.Context, ev *clientv3.Event) error, opts ...clientv3.OpOption) {
	var wch clientv3.WatchChan

watchLoop:
	if len(opts) > 0 {
		wch = client.Watch(ctx, node, opts...)
	} else {
		wch = client.Watch(ctx, node)
	}
	for {
		var wr clientv3.WatchResponse
		select {
		case wr = <-wch:
		case <-ctx.Done():
			lg.Info(exitMsg)
			return
		}
		if err := wr.Err(); err != nil {
			lg.Error("watch err", zap.Error(err))
			goto watchLoop
		}

		for _, ev := range wr.Events {
			if err := fn(ctx, ev); err != nil {
				lg.Error("fn err", zap.Error(err))
				time.Sleep(3 * time.Second)
				goto watchLoop
			}
		}
	}
}
