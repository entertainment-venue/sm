package server

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	var evPQ PriorityQueue
	heap.Init(&evPQ)

	ev := loadEvent{
		Service:     "foo.bar",
		Type:        evTypeContainerDel,
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
