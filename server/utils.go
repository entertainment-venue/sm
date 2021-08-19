package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

// 提出container和shard的公共属性
type admin struct {
	ctx    context.Context
	cancel context.CancelFunc

	id      string

	// https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
	wg sync.WaitGroup
}

func tickerLoop(ctx context.Context, duration time.Duration, exitMsg string, fn func(ctx context.Context) error, wg *sync.WaitGroup) {
	defer wg.Done()

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

func watchLoop(ctx context.Context, ew *etcdWrapper, node string, exitMsg string, fn func(ctx context.Context, ev *clientv3.Event) error, wg *sync.WaitGroup) {
	defer wg.Done()

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
