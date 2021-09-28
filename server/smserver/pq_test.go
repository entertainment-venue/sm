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
	"container/heap"
	"fmt"
	"testing"
)

func Test_PriorityQueue(t *testing.T) {
	var evPQ PriorityQueue
	heap.Init(&evPQ)

	ev := mvEvent{
		Service:     "foo.bar",
		Type:        tContainerDel,
		EnqueueTime: 1,
	}

	item := Item{
		Value:    ev.String(),
		Priority: 3,
		Index:    0,
	}
	heap.Push(&evPQ, &item)

	item2 := Item{
		Value:    ev.String(),
		Priority: 2,
		Index:    0,
	}
	heap.Push(&evPQ, &item2)

	item3 := Item{
		Value:    ev.String(),
		Priority: 1,
		Index:    0,
	}
	heap.Push(&evPQ, &item3)

	fmt.Println(fmt.Sprintf("%+v", evPQ.Pop()))
	fmt.Println(fmt.Sprintf("%+v", evPQ.Pop()))
	fmt.Println(fmt.Sprintf("%+v", evPQ.Pop()))
}
