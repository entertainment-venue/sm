package apputil

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type testShardImpl struct {
}

func (impl *testShardImpl) Add(ctx context.Context, id string, spec *ShardSpec) error {
	return nil
}
func (impl *testShardImpl) Drop(ctx context.Context, id string) error {
	return nil
}
func (impl *testShardImpl) Load(ctx context.Context, id string) (string, error) {
	return "", nil
}

func TestShardServer_NewShardServer_ParamError(t *testing.T) {
	var tests = []struct {
		opts   []ShardServerOption
		hasErr bool
	}{
		{
			opts: []ShardServerOption{
				ShardServerWithContainer(&Container{}),
				ShardServerWithLogger(ttLogger),
				ShardServerWithShardImplementation(&testShardImpl{}),
				ShardServerWithContext(context.Background()),
			},
			hasErr: true,
		},
		{
			opts: []ShardServerOption{
				ShardServerWithAddr("addr"),
				ShardServerWithLogger(ttLogger),
				ShardServerWithShardImplementation(&testShardImpl{}),
				ShardServerWithContext(context.Background()),
			},
			hasErr: true,
		},
		{
			opts: []ShardServerOption{
				ShardServerWithAddr("addr"),
				ShardServerWithContainer(&Container{}),
				ShardServerWithShardImplementation(&testShardImpl{}),
				ShardServerWithContext(context.Background()),
			},
			hasErr: true,
		},
		{
			opts: []ShardServerOption{
				ShardServerWithAddr("addr"),
				ShardServerWithContainer(&Container{}),
				ShardServerWithLogger(ttLogger),
				ShardServerWithContext(context.Background()),
			},
			hasErr: true,
		},
		{
			opts: []ShardServerOption{
				ShardServerWithAddr("addr"),
				ShardServerWithContainer(&Container{}),
				ShardServerWithLogger(ttLogger),
				ShardServerWithShardImplementation(&testShardImpl{}),
			},
			hasErr: true,
		},
	}

	for idx, tt := range tests {
		_, err := NewShardServer(tt.opts...)
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

func TestShardServer_NewShardServer_CancelCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ops := newTestContainerOptions(ctx)
	container, err := NewContainer(ops...)
	if err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	_, err = NewShardServer(
		ShardServerWithAddr(":8888"),
		ShardServerWithContainer(container),
		ShardServerWithLogger(ttLogger),
		ShardServerWithShardImplementation(&testShardImpl{}),
		ShardServerWithContext(ctx),
	)
	if err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(5 * time.Second)
	cancel()
	fmt.Println("waiting goroutine to exit...")
	time.Sleep(5 * time.Second)
}

func TestShardServer_NewShardServer_Close(t *testing.T) {
	ops := newTestContainerOptions(context.TODO())
	container, err := NewContainer(ops...)
	if err != nil {
		t.Errorf("unexpected err %s", err.Error())
		t.SkipNow()
	}

	ss, err := NewShardServer(
		ShardServerWithAddr(":8888"),
		ShardServerWithContainer(container),
		ShardServerWithLogger(ttLogger),
		ShardServerWithShardImplementation(&testShardImpl{}),
		ShardServerWithContext(context.TODO()),
	)
	if err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	go func() {
		<-ss.Done()
		fmt.Println("donec work")
	}()

	select {
	case <-time.After(5 * time.Second):
		ss.Close()
	}
	fmt.Println("waiting goroutine to exit...")
	time.Sleep(5 * time.Second)
}

func TestShardServer_Api(t *testing.T) {
	ops := newTestContainerOptions(context.TODO())
	container, err := NewContainer(ops...)
	if err != nil {
		t.Errorf("unexpected err %s", err.Error())
		t.SkipNow()
	}

	ss, err := NewShardServer(
		ShardServerWithAddr(":8888"),
		ShardServerWithContainer(container),
		ShardServerWithLogger(ttLogger),
		ShardServerWithShardImplementation(&testShardImpl{}),
		ShardServerWithContext(context.TODO()),
	)
	if err != nil {
		t.Errorf("err: %v", err)
		t.SkipNow()
	}

	time.Sleep(60 * time.Second)
	ss.Close()
}
