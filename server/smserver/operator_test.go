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
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"go.uber.org/zap"
)

var (
	ttLogger, _ = zap.NewProduction()
)

func Test_operator_moveActionList_sort(t *testing.T) {
	var tests = []struct {
		actual moveActionList
		expect moveActionList
	}{
		{
			actual: moveActionList{
				&moveAction{ShardId: "1"},
				&moveAction{ShardId: "2"},
			},
			expect: moveActionList{
				&moveAction{ShardId: "1"},
				&moveAction{ShardId: "2"},
			},
		},
		{
			actual: moveActionList{
				&moveAction{ShardId: "2"},
				&moveAction{ShardId: "1"},
			},
			expect: moveActionList{
				&moveAction{ShardId: "1"},
				&moveAction{ShardId: "2"},
			},
		},
	}
	for idx, tt := range tests {
		sort.Sort(tt.actual)
		if !reflect.DeepEqual(tt.actual, tt.expect) {
			fmt.Println(tt.actual)
			t.Errorf("idx %d unexpected", idx)
			t.SkipNow()
		}
	}
}

func Test_operator_move(t *testing.T) {
	o := operator{lg: ttLogger, service: "foo.bar"}
	o.httpClient = newHttpClient()

	time.Sleep(3 * time.Second)

	// ./etcdctl put /bd/app/foo.bar/task '[{"service":"foo.bar","shardId":"1","dropEndpoint":"","addEndpoint":"127.0.0.1:8889","allowDrop":false}]'
	value := `[{"service":"foo.bar","shardId":"1","dropEndpoint":"","addEndpoint":"127.0.0.1:8889","allowDrop":false}]`
	mal := moveActionList{}
	json.Unmarshal([]byte(value), &mal)
	o.move(moveActionList{})

	stopch := make(chan struct{})
	<-stopch
}

func Test_operator_dropOrAdd(t *testing.T) {
	o := operator{lg: ttLogger}
	o.httpClient = newHttpClient()

	ma := moveAction{
		Service:     "foo.bar",
		ShardId:     "1",
		AddEndpoint: "127.0.0.1:8889",
	}
	o.dropOrAdd(&ma)

	stopch := make(chan struct{})
	<-stopch
}

func Test_operator_send(t *testing.T) {
	o := operator{lg: ttLogger}
	o.httpClient = newHttpClient()

	if err := o.send("1", &storage.ShardSpec{}, "127.0.0.1:8889", "add"); err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	stopch := make(chan struct{})
	<-stopch
}
