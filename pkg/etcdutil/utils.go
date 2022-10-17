package etcdutil

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func WatchLoop(ctx context.Context, lg *zap.Logger, client EtcdWrapper, key string, rev int64, fn func(ctx context.Context, ev *clientv3.Event) error) {
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
				zap.Int64("CompactRevision", wr.CompactRevision),
				zap.Error(err),
			)
			// https://github.com/etcd-io/etcd/issues/8668
			if err == rpctypes.ErrCompacted {
				// 需要重新当前key的最新revision，修正startRev
				resp, err := client.Get(context.Background(), key, clientv3.WithPrefix())
				if err != nil {
					lg.Error(
						"WatchLoop try to get newest revision failed",
						zap.String("key", key),
						zap.Int64("startRev", startRev),
						zap.Error(err),
					)
				} else {
					lg.Info(
						"WatchLoop correct startRev",
						zap.String("key", key),
						zap.Int64("oldStartRev", startRev),
						zap.Int64("newStartRev", resp.Header.Revision+1),
					)
					startRev = resp.Header.Revision + 1
				}
			}
			time.Sleep(300 * time.Millisecond)
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
			}
		}

		// 发生错误时，从上次的rev开始watch
		startRev = wr.Header.GetRevision() + 1
	}
}
