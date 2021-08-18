package main

import (
	"context"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"time"
)

func waitTickerLoop(ctx context.Context, duration time.Duration, exitMsg string, fn func(ctx context.Context) error) {
	ticker := time.Tick(duration)
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			Logger.Printf(exitMsg)
			return
		}
		if err := fn(ctx); err != nil {
			Logger.Printf("err: %v", err)
		}
	}
}

func waitWatchLoop(ctx context.Context, ew *etcdWrapper, node string, exitMsg string, fn func(ctx context.Context, ev *clientv3.Event) error) {
	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())

watchLoop:
	wch := ew.etcdClientV3.Watch(ctx, node)
	for {
		var wr clientv3.WatchResponse
		select {
		case wr = <-wch:
		case <-ctx.Done():
			Logger.Printf(exitMsg)
			return
		}
		if err := wr.Err(); err != nil {
			Logger.Printf("err: %v", err)
			goto watchLoop
		}

		for _, ev := range wr.Events {
			if err := fn(ctx, ev); err != nil {
				Logger.Printf("err: %v", err)
				time.Sleep(defaultSleepTimeout)
				goto watchLoop
			}
		}
	}
}

func checkShardOwner(ctx context.Context, ew *etcdWrapper, hbContainerNode string, hbShardNode string) error {
	containers, err := ew.getKvs(ctx, hbContainerNode)
	if err != nil {
		return errors.Wrap(err, "")
	}

	shards, err := ew.getKvs(ctx, hbShardNode)
	if err != nil {
		return errors.Wrap(err, "")
	}

	for _, v := range shards {
		var shb ShardHeartbeat
		if err := json.Unmarshal([]byte(v), &shb); err != nil {
			return errors.Wrap(err, "")
		}
		if _, ok := containers[shb.ContainerId]; !ok {
			break
		}
	}
	return nil
}
