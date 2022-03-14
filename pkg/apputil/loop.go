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

	"github.com/entertainment-venue/sm/pkg/etcdutil"
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

func WatchLoop(ctx context.Context, lg *zap.Logger, client etcdutil.EtcdWrapper, key string, rev int64, fn func(ctx context.Context, ev *clientv3.Event) error) {
	var (
		startRev int64
		opts     []clientv3.OpOption
		wch      clientv3.WatchChan
	)
	startRev = rev

loop:
	lg.Info(
		"WatchLoop start",
		zap.String("key", key),
		zap.Int64("startRev", startRev),
	)

	opts = append(opts, clientv3.WithPrefix())
	// 允许不关注rev的watch
	if startRev >= 0 {
		opts = append(opts, clientv3.WithRev(startRev))

		// delete事件需要上一个kv
		// https://github.com/etcd-io/etcd/issues/6120
		opts = append(opts, clientv3.WithPrevKV())
	}
	wch = client.Watch(ctx, key, opts...)
	for {
		var wr clientv3.WatchResponse
		select {
		case wr = <-wch:
		case <-ctx.Done():
			lg.Info(
				"WatchLoop exit",
				zap.String("key", key),
				zap.Int64("startRev", startRev),
			)
			return
		}
		if err := wr.Err(); err != nil {
			lg.Error(
				"WatchLoop error",
				zap.String("key", key),
				zap.Int64("startRev", startRev),
				zap.Error(err),
			)
			goto loop
		}

		for _, ev := range wr.Events {
			if err := fn(ctx, ev); err != nil {
				lg.Error(
					"WatchLoop error when call fn",
					zap.String("key", key),
					zap.Int64("startRev", startRev),
					zap.Error(err),
				)
				time.Sleep(3 * time.Second)
				goto loop
			}
		}

		// 发生错误时，从上次的rev开始watch
		startRev = wr.Header.GetRevision() + 1
	}
}
