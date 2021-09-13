package server

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func Test_init(t *testing.T) {
	ctr, err := newServerContainer(context.TODO(), "127.0.0.1:8888", "foo.bar", []string{"127.0.0.1:2379"})
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	if err := ctr.leader.init(); err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}
}

func Test_newLeader(t *testing.T) {
	ctr, err := newServerContainer(context.TODO(), "127.0.0.1:8888", "foo.bar", []string{"127.0.0.1:2379"})
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	go func() {
		time.Sleep(5 * time.Second)
		ctr.leader.close()
	}()

	select {
	case <-ctr.leader.ctx.Done():
		fmt.Println("exit")
	}
}
