package commonutil

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func testFunc(ctx context.Context) {
	fmt.Println("fn in")
	select {
	case <-ctx.Done():
	}
	fmt.Println("fn out")
}

func Test_GoroutineStopper_Start(t *testing.T) {
	gs := GoroutineStopper{}
	gs.Wrap(testFunc)

	time.Sleep(5 * time.Second)
	fmt.Println("call Close func")
	gs.Close()
}
