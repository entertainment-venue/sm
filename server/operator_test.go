package server

import (
	"context"
	"testing"
	"time"
)

func Test_newOperator(t *testing.T) {
	ctr, err := newContainer(context.TODO(), "127.0.0.1:8888", "foo.bar", []string{"127.0.0.1:2379"})
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	op, err := newOperator(ctr, "foo.bar")
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	go func() {
		time.Sleep(60 * time.Second)
		op.Close()
	}()
}

func Test_remove(t *testing.T) {
	ctr, err := newContainer(context.TODO(), "127.0.0.1:8888", "foo.bar", []string{"127.0.0.1:2379"})
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	o := operator{}
	o.sc = ctr

	if err := o.remove(context.TODO(), "1", "foo.bar"); err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}
}

func Test_dropAndAdd(t *testing.T) {
	ctr, err := newContainer(context.TODO(), "127.0.0.1:8888", "foo.bar", []string{"127.0.0.1:2379"})
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	o := operator{}
	o.sc = ctr
	o.httpClient = newHttpClient()

	// ma := moveAction{
	// 	Service:     "foo.bar",
	// 	ShardId:     "1",
	// 	AddEndpoint: "127.0.0.1:8888",
	// }
	// o.dropAndAdd(&ma)

	ma := moveAction{
		Service:     "foo.bar",
		ShardId:     "1",
		AddEndpoint: "127.0.0.1:8888",
		AllowDrop:   true,
	}
	o.dropAndAdd(context.TODO(), &ma)
}
