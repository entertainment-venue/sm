package apputil

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil/core"
	"go.uber.org/zap"
)

var (
	ttLogger, _ = zap.NewProduction()
)

func Test_Container_NewContainer_ParamErr(t *testing.T) {
	var tests = []struct {
		opts   []ContainerOption
		hasErr bool
	}{
		{
			opts: []ContainerOption{
				WithService("service"),
				WithEndpoints([]string{"127.0.0.1:8888"}),
				WithLogger(ttLogger),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				WithId("id"),
				WithEndpoints([]string{"127.0.0.1:8888"}),
				WithLogger(ttLogger),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				WithId("id"),
				WithService("service"),
				WithLogger(ttLogger),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				WithId("id"),
				WithService("service"),
				WithEndpoints([]string{"127.0.0.1:8888"}),
			},
			hasErr: true,
		},
		{
			opts: []ContainerOption{
				WithId("id"),
				WithService("service"),
				WithEndpoints([]string{"127.0.0.1:8888"}),
				WithLogger(ttLogger),
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

func Test_Container_NewContainer_CancelCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ops := newTestContainerOptions(ctx)
	if _, err := NewContainer(ops...); err != nil {
		t.Errorf("unexpected err %s", err.Error())
		t.SkipNow()
	}

	time.Sleep(100 * time.Second)

	select {
	case <-time.After(5 * time.Second):
		cancel()
	}
	fmt.Println("waiting goroutine to exit...")
	time.Sleep(5 * time.Second)
}

func Test_Container_Close(t *testing.T) {
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
		WithId("127.0.0.1:8888"),
		WithService("foo.bar"),
		WithEndpoints([]string{"127.0.0.1:8888"}),
		WithLogger(ttLogger),
		WithShardPrimitives(&core.ShardKeeper{}),
	}
}
