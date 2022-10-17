package etcdutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	ttLogger, _ = zap.NewProduction()
)

func Test_WatchLoop(t *testing.T) {
	client, err := NewEtcdClient([]string{"127.0.0.1:2379"}, ttLogger)
	if err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	resp, err := client.GetKV(context.TODO(), "foo", nil)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	fmt.Println(resp.Header.GetRevision())

	WatchLoop(
		context.TODO(),
		ttLogger,
		client,
		"foo",
		resp.Header.GetRevision()+1,
		func(ctx context.Context, ev *clientv3.Event) error {
			fmt.Println(ev.Type, ev.Kv.CreateRevision, ev.Kv.ModRevision)
			return nil
		},
	)
}

func Test_WatchLoop_close(t *testing.T) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)

	client, err := NewEtcdClient([]string{"127.0.0.1:2379"}, ttLogger)
	if err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	wg.Add(1)
	go WatchLoop(
		ctx,
		ttLogger,
		client,
		"foo",
		0,
		func(ctx context.Context, ev *clientv3.Event) error {
			fmt.Println(ev.Type)
			return nil
		},
	)

	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				cancel()
			}
		}
	}()

	wg.Wait()
	fmt.Println("TestWatchLoop exit")
}
