package main

import (
	"context"
	"github.com/pkg/errors"
	"time"

	"github.com/coreos/etcd/clientv3/concurrency"
)

type shard struct {
	id      string
	service string
}

func (s *shard) heartbeat(ctx context.Context) error {
	ew, err := newEtcdWrapper([]string{})
	if err != nil {
		return errors.Wrap(err, "")
	}

	for {
	hbLoop:
		select {
		case <-ctx.Done():
			Logger.Printf("heartbeat exit")
			return nil
		default:
		}

		_, err := concurrency.NewSession(ew.etcdClientV3, concurrency.WithTTL(defaultSessionTimeout))
		if err != nil {
			Logger.Printf("err %+v", err)
			time.Sleep(defaultSleepTimeout)
			goto hbLoop
		}

	}
}
