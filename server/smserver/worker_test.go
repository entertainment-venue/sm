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
	"reflect"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func Test_newMaintenanceWorker(t *testing.T) {
	ctr, err := newSMContainer(ttLogger, "127.0.0.1:8888", "foo.bar", nil)
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	mw, _ := newWorker(context.TODO(), ttLogger, ctr, "foo.bar")

	time.Sleep(5 * time.Second)
	mw.Close()
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
	mw := Worker{}
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
		fixShardIdAndManualContainerId ArmorMap
		hbContainerIdAndAny            ArmorMap
		hbShardIdAndContainerId        ArmorMap
		expect                         moveActionList
	}{
		// container新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
			},
			expect: moveActionList{
				&moveAction{Service: service, ShardId: "s3", AddEndpoint: "c2", AllowDrop: false},
			},
		},

		// container新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
			},
			expect: moveActionList{
				&moveAction{Service: service, ShardId: "s1", DropEndpoint: "c1", AddEndpoint: "c2", AllowDrop: false},
			},
		},

		// container存活，没有shard需要移动
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c2",
			},
			expect: nil,
		},
		// container存活，没有shard需要移动，和顺序无关
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c2",
				"s2": "c1",
			},
			expect: nil,
		},
		// container存活，没有shard需要移动，和顺序无关
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "",
			},
			expect: nil,
		},

		// container不存活，数据不一致不处理
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c2",
			},
			expect: nil,
		},
	}

	logger, _ := zap.NewDevelopment()
	w := Worker{service: "foo.bar", lg: logger}

	for idx, tt := range tests {
		r := w.reallocate(tt.fixShardIdAndManualContainerId, tt.hbContainerIdAndAny, tt.hbShardIdAndContainerId)
		if !reflect.DeepEqual(r, tt.expect) {
			t.Errorf("idx: %d actual: %s, expect: %s", idx, r.String(), tt.expect.String())
			t.SkipNow()
		}
	}
}

func Test_shardLoadChecker(t *testing.T) {
	eq := newTaskProcessor(ttLogger, nil)

	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		Kv:   &mvccpb.KeyValue{},
	}

	mw := Worker{}

	if err := mw.shardLoadChecker(context.TODO(), &ev); err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(5 * time.Second)

	eq.Close()
}

func Test_containerLoadChecker(t *testing.T) {
	eq := newTaskProcessor(ttLogger, nil)

	ev := clientv3.Event{
		Type: mvccpb.DELETE,
		Kv:   &mvccpb.KeyValue{},
	}

	mw := Worker{}

	if err := mw.containerLoadChecker(context.TODO(), &ev); err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(5 * time.Second)

	eq.Close()
}
