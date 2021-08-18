package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

type ShardHeartbeat struct {
	ContainerId string `json:"container_id"`
	Load        string `json:"load"`
	Timestamp   int64  `json:"timestamp"`
}

type ContainerHeartbeat struct {
	Timestamp int64 `json:"timestamp"`
}

type AppSpec struct {
}

type etcdWrapper struct {
	etcdClientV3 *clientv3.Client

	container *container
}

func newEtcdWrapper(endpoints []string) (*etcdWrapper, error) {
	if len(endpoints) < 1 {
		return nil, errors.New("You must provide at least one etcd address")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &etcdWrapper{etcdClientV3: client}, nil
}

func (w *etcdWrapper) nodePrefix(admin bool) string {
	if admin {
		return fmt.Sprintf("/borderland/admin")
	} else {
		return fmt.Sprintf("/borderland/app/%s", w.container.application)
	}
}

// /borderland/sfmq_proxy/admin/leader
func (w *etcdWrapper) leaderNode() string {
	return fmt.Sprintf("%s/leader", w.nodePrefix(true))
}

// /borderland/sfmq_proxy/admin/shardhb/uuid
func (w *etcdWrapper) heartbeatShardIdNode(shardId string, admin bool) string {
	return fmt.Sprintf("%s/shardhb/%s", w.nodePrefix(admin), shardId)
}

// /borderland/sfmq_proxy/admin/shardhb/
func (w *etcdWrapper) heartbeatShardNode(admin bool) string {
	return fmt.Sprintf("%s/shardhb/", w.nodePrefix(admin))
}

// /borderland/sfmq_proxy/admin/containerhb/uuid
func (w *etcdWrapper) heartbeatContainerIdNode(containerId string, admin bool) string {
	return fmt.Sprintf("%s/containerhb/%s", w.nodePrefix(admin), containerId)
}

// /borderland/sfmq_proxy/admin/containerhb/
func (w *etcdWrapper) heartbeatContainerNode(admin bool) string {
	return fmt.Sprintf("%s/containerhb/", w.nodePrefix(admin))
}

func (w *etcdWrapper) appSpecNode() string {
	return fmt.Sprintf("%s/spec", w.container.application)
}

func (w *etcdWrapper) get(ctx context.Context, node string, opts []clientv3.OpOption) (*clientv3.GetResponse, error) {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.etcdClientV3.Get(timeoutCtx, node, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return resp, nil
}

func (w *etcdWrapper) getKvs(ctx context.Context, prefix string) (map[string]string, error) {
	// https://github.com/etcd-io/etcd/blob/master/tests/integration/clientv3/kv_test.go
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	resp, err := w.get(ctx, prefix, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "FAILED to get prefix %s", prefix)
	}
	if resp.Count == 0 {
		return nil, nil
	}

	r := make(map[string]string)
	for _, kv := range resp.Kvs {
		_, file := filepath.Split(string(kv.Key))
		r[file] = string(kv.Value)
	}
	return r, nil
}
