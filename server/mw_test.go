package server

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

func Test_Start(t *testing.T) {
	ctr, err := newServerContainer(context.TODO(), "127.0.0.1:8888", "foo.bar", []string{"127.0.0.1:2379"})
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	mw := newMaintenanceWorker(ctr, "foo.bar")
	mw.Start()

	go func() {
		time.Sleep(5 * time.Second)
		mw.Close()
	}()

	select {
	case <-mw.ctx.Done():
		fmt.Printf("exit")
	}
}
func Test_containerChanged(t *testing.T) {
	var tests = []struct {
		fixContainerIds     []string
		surviveContainerIds []string
		expect              bool
	}{
		{
			fixContainerIds:     []string{},
			surviveContainerIds: []string{},
			expect:              false,
		},
		{
			fixContainerIds:     []string{"1", "2"},
			surviveContainerIds: []string{"1", ""},
			expect:              true,
		},
		{
			fixContainerIds:     []string{"1", "2"},
			surviveContainerIds: []string{"2", "1"},
			expect:              false,
		},
		{
			fixContainerIds:     []string{"1", "2"},
			surviveContainerIds: []string{"1", "2", "3"},
			expect:              true,
		},
	}

	for idx, tt := range tests {
		if tt.expect != containerChanged(tt.fixContainerIds, tt.surviveContainerIds) {
			t.Errorf("idx %d expect %t", idx, tt.expect)
			t.SkipNow()
		}
	}
}

func Test_shardChanged(t *testing.T) {
	var tests = []struct {
		fixShardIds       []string
		surviveShardIdMap map[string]struct{}
		expect            bool
	}{
		{
			fixShardIds:       []string{},
			surviveShardIdMap: map[string]struct{}{},
			expect:            false,
		},
		{
			fixShardIds: []string{"1"},
			surviveShardIdMap: map[string]struct{}{
				"1": {},
			},
			expect: false,
		},
		{
			fixShardIds:       []string{"1"},
			surviveShardIdMap: map[string]struct{}{},
			expect:            true,
		},
		{
			fixShardIds: []string{"1", "2"},
			surviveShardIdMap: map[string]struct{}{
				"1": {},
				"2": {},
			},
			expect: false,
		},
	}
	for idx, tt := range tests {
		if tt.expect != shardChanged(tt.fixShardIds, tt.surviveShardIdMap) {
			t.Errorf("idx %d expect %t", idx, tt.expect)
			t.SkipNow()
		}
	}
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
		sort.Sort(r)
		sort.Sort(tt.expect)
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
