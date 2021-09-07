package server

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"reflect"
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

func Test_reallocate(t *testing.T) {
	service := "foo.bar"
	var tests = []struct {
		surviveContainerIdAndValue map[string]string
		fixShardIdAndContainerId   map[string]string
		expect                     moveActionList
	}{
		// container存活，没有shard需要移动
		{
			surviveContainerIdAndValue: map[string]string{
				"containerA": "",
				"containerB": "",
			},
			fixShardIdAndContainerId: map[string]string{
				"shard1": "containerA",
				"shard2": "containerB",
			},
			expect: nil,
		},

		// container存活，有shard当前没有container
		{
			surviveContainerIdAndValue: map[string]string{
				"containerA": "",
				"containerB": "",
			},
			fixShardIdAndContainerId: map[string]string{
				"shard1": "containerA",
				"shard2": "",
			},
			expect: moveActionList{
				&moveAction{Service: service, ShardId: "shard2", AddEndpoint: "containerB"},
			},
		},

		// container存活，shard的container调整
		{
			surviveContainerIdAndValue: map[string]string{
				"containerA": "",
				"containerB": "",
			},
			fixShardIdAndContainerId: map[string]string{
				"shard1": "containerB",
				"shard2": "containerA",
			},
			expect: moveActionList{
				&moveAction{Service: service, ShardId: "shard1", DropEndpoint: "containerB", AddEndpoint: "containerA"},
				&moveAction{Service: service, ShardId: "shard2", DropEndpoint: "containerA", AddEndpoint: "containerB"},
			},
		},

		// container不存活，shard的container变更
		{
			surviveContainerIdAndValue: map[string]string{
				"containerB": "",
			},
			fixShardIdAndContainerId: map[string]string{
				"shard1": "containerA",
				"shard2": "containerB",
			},
			expect: moveActionList{
				&moveAction{Service: service, ShardId: "shard1", DropEndpoint: "containerA", AddEndpoint: "containerB", AllowDrop: true},
			},
		},
	}

	for idx, tt := range tests {
		r := reallocate(service, tt.surviveContainerIdAndValue, tt.fixShardIdAndContainerId)
		if !reflect.DeepEqual(r, tt.expect) {
			t.Errorf("idx: %d actual: %s, expect: %s", idx, r.String(), tt.expect.String())
			t.SkipNow()
		}
	}
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

func Test_containerLoadChecker(t *testing.T) {
	eq := newEventQueue(context.Background())

	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		Kv:   &mvccpb.KeyValue{},
	}

	if err := containerLoadChecker(context.TODO(), "foo.bar", eq, &ev); err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(5 * time.Second)

	eq.Close()
}
