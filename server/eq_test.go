package server

import (
	"context"
	"testing"
)

func TestEventQueue_push(t *testing.T) {
	lev := loadEvent{
		Service:     "foo.bar",
		Type:        evTypeContainerDel,
		EnqueueTime: 1,
	}
	item := Item{
		Value:    lev.String(),
		Priority: 0,
	}

	eq := newEventQueue(context.TODO())
	eq.push(&item, true)
}
