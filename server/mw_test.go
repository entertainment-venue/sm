package server

import (
	"fmt"
	"testing"
	"time"
)

func Test_Start(t *testing.T) {
	ctr, err := newContainer("127.0.0.1:8888", "foo.bar", []string{"127.0.0.1:2379"})
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
