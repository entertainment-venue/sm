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
