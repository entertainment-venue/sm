package server

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"sync"
	"testing"
	"time"
)

func Test_tickerLoop(t *testing.T) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)

	wg.Add(1)
	go tickerLoop(
		ctx,
		time.Second,
		"test loop exit",
		func(ctx context.Context) error {
			fmt.Println("test fn " + time.Now().String())
			return nil
		},
		&wg,
	)

	go func() {
		for {
			select {
			case <-time.After(3 * time.Second):
				cancel()
			}
		}
	}()

	wg.Wait()
	fmt.Println("TestTickerLoop exit")
}

func Test_watchLoop(t *testing.T) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)

	ew, err := newEtcdWrapper([]string{"127.0.0.1:2379"}, &container{})
	if err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	wg.Add(1)
	go watchLoop(
		ctx,
		ew,
		"foo",
		"test loop exit",
		func(ctx context.Context, ev *clientv3.Event) error {
			fmt.Println(ev.Type)
			return nil
		},
		&wg,
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

func Test_shardLoadChecker(t *testing.T) {
	eq := newEventQueue(context.Background())

	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		Kv:   &mvccpb.KeyValue{},
	}

	if err := shardLoadChecker(context.TODO(), "foo.bar", eq, &ev); err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(5 * time.Second)

	eq.Close()
}
