package server

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
)

// 提出container和shard的公共属性
// 抽象数据结构，也会引入数据结构之间的耦合
type goroutineStopper struct {
	// https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
	// goroutineStopper close
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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
	wch := ew.client.Watch(ctx, node)
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

type ArmorMap map[string]string

func (m ArmorMap) KeyList() []string {
	var r []string
	for k := range m {
		r = append(r, k)
	}
	return r
}

func (m ArmorMap) KeyMap() map[string]struct{} {
	r := make(map[string]struct{})
	for k := range m {
		r[k] = struct{}{}
	}
	return r
}

func (m ArmorMap) ValueList() []string {
	var r []string
	for _, v := range m {
		if v == "" {
			continue
		}
		r = append(r, v)
	}
	return r
}
