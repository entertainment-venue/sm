package apputil

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestContainer_NewContainer_ParamErr(t *testing.T) {
	var tests = []struct {
		opts   []ContainerOption
		hasErr bool
	}{
		{
			opts: []ContainerOption{
				ContainerWithService("service"),
				ContainerWithEndpoints([]string{"127.0.0.1:8888"}),
				ContainerWithLogger(ttLogger),
				ContainerWithContext(context.TODO()),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				ContainerWithId("id"),
				ContainerWithEndpoints([]string{"127.0.0.1:8888"}),
				ContainerWithLogger(ttLogger),
				ContainerWithContext(context.TODO()),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				ContainerWithId("id"),
				ContainerWithService("service"),
				ContainerWithLogger(ttLogger),
				ContainerWithContext(context.TODO()),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				ContainerWithId("id"),
				ContainerWithService("service"),
				ContainerWithEndpoints([]string{"127.0.0.1:8888"}),
				ContainerWithContext(context.TODO()),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				ContainerWithId("id"),
				ContainerWithService("service"),
				ContainerWithEndpoints([]string{"127.0.0.1:8888"}),
				ContainerWithLogger(ttLogger),
			},
			hasErr: true,
		},
	}

	for idx, tt := range tests {
		_, err := NewContainer(tt.opts...)
		if tt.hasErr {
			if err == nil {
				t.Errorf("idx %d expect err", idx)
				t.SkipNow()
			} else {
				if !strings.HasSuffix(err.Error(), " err") {
					t.Errorf("idx %d expect err suffix", idx)
					t.SkipNow()
				}
			}
		}
	}
}

func TestContainer_NewContainer_CancelCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ops := newTestContainerOptions(ctx)
	if _, err := NewContainer(ops...); err != nil {
		t.Errorf("unexpected err %s", err.Error())
		t.SkipNow()
	}

	select {
	case <-time.After(5 * time.Second):
		cancel()
	}
	fmt.Println("waiting goroutine to exit...")
	time.Sleep(5 * time.Second)
}

func TestContainer_Close(t *testing.T) {
	ops := newTestContainerOptions(context.TODO())
	container, err := NewContainer(ops...)
	if err != nil {
		t.Errorf("unexpected err %s", err.Error())
		t.SkipNow()
	}

	go func() {
		<-container.Done()
		fmt.Println("donec work")
	}()

	select {
	case <-time.After(5 * time.Second):
		container.Close()
	}
	fmt.Println("waiting goroutine to exit...")
	time.Sleep(5 * time.Second)
}

func newTestContainerOptions(ctx context.Context) []ContainerOption {
	return []ContainerOption{
		ContainerWithId("127.0.0.1:8888"),
		ContainerWithService("foo.bar"),
		ContainerWithEndpoints([]string{"127.0.0.1:2379"}),
		ContainerWithLogger(ttLogger),
		ContainerWithContext(ctx),
	}
}
