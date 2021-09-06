package server

import (
	"fmt"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	var evPQ PriorityQueue

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
	evPQ = append(evPQ, &item)

	fmt.Println(evPQ.Pop())
}
