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

	"github.com/coreos/etcd/clientv3"
	"github.com/entertainment-venue/sm/pkg/logutil"
)

func TickerLoop(ctx context.Context, duration time.Duration, exitMsg string, fn func(ctx context.Context) error) {
	ticker := time.Tick(duration)
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			logutil.Logger.Printf(exitMsg)
			return
		}
		if err := fn(ctx); err != nil {
			logutil.Logger.Printf("err: %v", err)
		}
	}
}

func WatchLoop(ctx context.Context, client *clientv3.Client, node string, exitMsg string, fn func(ctx context.Context, ev *clientv3.Event) error) {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())

watchLoop:
	wch := client.Watch(ctx, node)
	for {
		var wr clientv3.WatchResponse
		select {
		case wr = <-wch:
		case <-ctx.Done():
			logutil.Logger.Printf(exitMsg)
			return
		}
		if err := wr.Err(); err != nil {
			logutil.Logger.Printf("err: %v", err)
			goto watchLoop
		}

		for _, ev := range wr.Events {
			if err := fn(ctx, ev); err != nil {
				logutil.Logger.Printf("err: %v", err)
				time.Sleep(3 * time.Second)
				goto watchLoop
			}
		}
	}
}
