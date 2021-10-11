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

package smserver

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

func Test_newMaintenanceWorker(t *testing.T) {
	ctr, err := newSMContainer(context.TODO(), ttLogger, "127.0.0.1:8888", "foo.bar", nil)
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	mw := newMaintenanceWorker(context.TODO(), ttLogger, ctr, "foo.bar")

	go func() {
		time.Sleep(5 * time.Second)
		mw.Close()
	}()

	select {
	case <-mw.ctx.Done():
		fmt.Printf("exit")
	}
}

func Test_changed(t *testing.T) {
	var tests = []struct {
		a      []string
		b      []string
		expect bool
	}{
		{
			a:      []string{},
			b:      []string{},
			expect: false,
		},
		{
			a:      []string{"1"},
			b:      []string{"1"},
			expect: false,
		},
		{
			a:      []string{"1"},
			b:      []string{},
			expect: true,
		},
		{
			a:      []string{"1", "2"},
			b:      []string{"2", "1"},
			expect: false,
		},
		{
			a:      []string{"1", "2"},
			b:      []string{"1", "2", "3"},
			expect: true,
		},
	}
	mw := maintenanceWorker{}
	for idx, tt := range tests {
		if tt.expect != mw.changed(tt.a, tt.b) {
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

	w := maintenanceWorker{}

	for idx, tt := range tests {
		// TODO 修正test case
		r := w.reallocate(nil, tt.surviveContainerIdAndValue, tt.fixShardIdAndContainerId)
		sort.Sort(r)
		sort.Sort(tt.expect)
		if !reflect.DeepEqual(r, tt.expect) {
			t.Errorf("idx: %d actual: %s, expect: %s", idx, r.String(), tt.expect.String())
			t.SkipNow()
		}
	}
}

func Test_shardLoadChecker(t *testing.T) {
	eq := newEventQueue(context.Background(), ttLogger, nil)

	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		Kv:   &mvccpb.KeyValue{},
	}

	mw := maintenanceWorker{}

	if err := mw.shardLoadChecker(context.TODO(), &ev); err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(5 * time.Second)

	eq.Close()
}

func Test_containerLoadChecker(t *testing.T) {
	eq := newEventQueue(context.Background(), ttLogger, nil)

	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		Kv:   &mvccpb.KeyValue{},
	}

	mw := maintenanceWorker{}

	if err := mw.containerLoadChecker(context.TODO(), &ev); err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(5 * time.Second)

	eq.Close()
}

func Test_extractNeedAssignShardIds(t *testing.T) {
	tests := []struct {
		shardIdAndManualContainerId  ArmorMap
		surviveContainerIdAndAny     ArmorMap
		surviveShardIdAndContainerId ArmorMap
		expectPrepare                []string
		expectMAL                    moveActionList
	}{
		{
			shardIdAndManualContainerId: ArmorMap{
				"s1": "c1",
			},
			surviveContainerIdAndAny:     ArmorMap{},
			surviveShardIdAndContainerId: ArmorMap{},
			expectPrepare:                nil,
			expectMAL: moveActionList{
				&moveAction{
					ShardId:     "s1",
					AddEndpoint: "c1",
				},
			},
		},
		{
			shardIdAndManualContainerId: ArmorMap{
				"s1": "",
			},
			surviveContainerIdAndAny:     ArmorMap{},
			surviveShardIdAndContainerId: ArmorMap{},
			expectPrepare:                []string{"s1"},
			expectMAL:                    nil,
		},
		{
			shardIdAndManualContainerId: ArmorMap{
				"s1": "c1",
			},
			surviveContainerIdAndAny: ArmorMap{},
			surviveShardIdAndContainerId: ArmorMap{
				"s1": "c2",
			},
			expectPrepare: nil,
			expectMAL: moveActionList{
				&moveAction{
					ShardId:      "s1",
					DropEndpoint: "c2",
					AddEndpoint:  "c1",
				},
			},
		},
		{
			shardIdAndManualContainerId: ArmorMap{
				"s1": "c1",
			},
			surviveContainerIdAndAny: ArmorMap{},
			surviveShardIdAndContainerId: ArmorMap{
				"s1": "c2",
			},
			expectPrepare: nil,
			expectMAL: moveActionList{
				&moveAction{
					ShardId:      "s1",
					DropEndpoint: "c2",
					AddEndpoint:  "c1",
				},
			},
		},
		{
			shardIdAndManualContainerId: ArmorMap{
				"s1": "",
			},
			surviveContainerIdAndAny: ArmorMap{},
			surviveShardIdAndContainerId: ArmorMap{
				// c1不存在于surviveContainerIdAndAny
				"s1": "c1",
			},
			expectPrepare: []string{"s1"},
			expectMAL:     nil,
		},
		{
			shardIdAndManualContainerId: ArmorMap{
				"s1": "",
			},
			surviveContainerIdAndAny: ArmorMap{
				"c1": "",
			},
			surviveShardIdAndContainerId: ArmorMap{
				// c2存在于surviveContainerIdAndAny
				"s1": "c1",
			},
			expectPrepare: nil,
			expectMAL:     nil,
		},

		// 删除case
		{
			shardIdAndManualContainerId: ArmorMap{
				"s1": "",
			},
			surviveContainerIdAndAny: ArmorMap{
				"c1": "",
			},
			surviveShardIdAndContainerId: ArmorMap{
				// c2存在于surviveContainerIdAndAny
				"s1": "c1",
				"s2": "c1",
			},
			expectPrepare: nil,
			expectMAL: moveActionList{
				&moveAction{
					ShardId:      "s2",
					DropEndpoint: "c1",
				},
			},
		},
	}
	mw := maintenanceWorker{}
	for idx, tt := range tests {
		actualPrepare, actualMAL := mw.extractNeedAssignShardIds(tt.shardIdAndManualContainerId, tt.surviveContainerIdAndAny, tt.surviveShardIdAndContainerId)
		if !reflect.DeepEqual(actualPrepare, tt.expectPrepare) {
			t.Errorf("idx %d expect %+v actual %+v", idx, tt.expectPrepare, actualPrepare)
			t.SkipNow()
		}
		if !reflect.DeepEqual(actualMAL, tt.expectMAL) {
			t.Errorf("idx %d expect %+v actual %+v", idx, tt.expectMAL, actualMAL)
			t.SkipNow()
		}
	}
}

func Test_parseAssignment(t *testing.T) {
	tests := []struct {
		assignment                   map[string][]string
		surviveContainerIdAndAny     ArmorMap
		surviveShardIdAndContainerId ArmorMap
		expectMAL                    moveActionList
	}{
		{
			assignment: map[string][]string{
				"c1": []string{"s1"},
			},
			surviveShardIdAndContainerId: ArmorMap{},
			expectMAL: moveActionList{
				&moveAction{
					ShardId:     "s1",
					AddEndpoint: "c1",
				},
			},
		},
		{
			assignment: map[string][]string{
				"c1": {"s1"},
			},
			surviveShardIdAndContainerId: ArmorMap{
				"s1": "c2",
			},
			expectMAL: moveActionList{
				&moveAction{
					ShardId:      "s1",
					DropEndpoint: "c2",
					AddEndpoint:  "c1",
					AllowDrop:    true,
				},
			},
		},
		{
			assignment: map[string][]string{
				"c1": {"s1"},
			},
			surviveContainerIdAndAny: ArmorMap{
				"c2": "",
			},
			surviveShardIdAndContainerId: ArmorMap{
				"s1": "c2",
			},
			expectMAL: moveActionList{
				&moveAction{
					ShardId:      "s1",
					DropEndpoint: "c2",
					AddEndpoint:  "c1",
					AllowDrop:    false,
				},
			},
		},
	}
	mw := maintenanceWorker{}
	for idx, tt := range tests {
		actualMAL := mw.parseAssignment(tt.assignment, tt.surviveContainerIdAndAny, tt.surviveShardIdAndContainerId)
		if !reflect.DeepEqual(actualMAL, tt.expectMAL) {
			t.Errorf("idx %d expect %+v actual %+v", idx, tt.expectMAL, actualMAL)
			t.SkipNow()
		}
	}
}
