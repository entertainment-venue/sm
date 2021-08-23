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
	ContainerId string `json:"containerId"`
	Load        string `json:"sysLoad"`
	Timestamp   int64  `json:"timestamp"`
}

type etcdWrapper struct {
	client *clientv3.Client

	cr *container
}

func newEtcdWrapper(endpoints []string, cr *container) (*etcdWrapper, error) {
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
	return &etcdWrapper{client: client, cr: cr}, nil
}

func (w *etcdWrapper) nodePrefix(admin bool) string {
	if admin {
		return fmt.Sprintf("/borderland/admin")
	} else {
		return fmt.Sprintf("/borderland/app/%s", w.cr.service)
	}
}

// /borderland/proxy/admin/leader
func (w *etcdWrapper) leaderNode() string {
	return fmt.Sprintf("%s/leader", w.nodePrefix(true))
}

// /borderland/proxy/admin/shardhb/uuid
func (w *etcdWrapper) hbShardIdNode(id string, admin bool) string {
	return fmt.Sprintf("%s/shardhb/%s", w.nodePrefix(admin), id)
}

// /borderland/proxy/admin/shardhb/
func (w *etcdWrapper) hbShardNode(admin bool) string {
	return fmt.Sprintf("%s/shardhb/", w.nodePrefix(admin))
}

// /borderland/proxy/admin/containerhb/uuid
func (w *etcdWrapper) hbContainerIdNode(id string, admin bool) string {
	return fmt.Sprintf("%s/containerhb/%s", w.nodePrefix(admin), id)
}

// /borderland/proxy/admin/containerhb/
func (w *etcdWrapper) hbContainerNode(admin bool) string {
	return fmt.Sprintf("%s/containerhb/", w.nodePrefix(admin))
}

// /borderland/app/proxy/shard/业务自己定义的shard id
func (w *etcdWrapper) appShardIdNode(id string) string {
	return fmt.Sprintf("%s/shard/%s", w.nodePrefix(false), id)
}

func (w *etcdWrapper) appShardNode() string {
	return fmt.Sprintf("%s/shard/", w.nodePrefix(false))
}

// /borderland/app/proxy/spec 存储app的基本信息
func (w *etcdWrapper) appSpecNode() string {
	return fmt.Sprintf("%s/spec", w.nodePrefix(false))
}

// /borderland/proxy/task
// 如果app的task节点存在任务，不能产生新的新的任务，必须等待ack完成
func (w *etcdWrapper) appTaskNode() string {
	return fmt.Sprintf("%s/task", w.nodePrefix(false))
}

func (w *etcdWrapper) get(_ context.Context, node string, opts []clientv3.OpOption) (*clientv3.GetResponse, error) {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.client.Get(timeoutCtx, node, opts...)
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

func (w *etcdWrapper) compareAndSwap(_ context.Context, node string, curValue string, newValue string, ttl int64) (string, error) {
	if curValue == "" || newValue == "" {
		return "", errors.Errorf("FAILED node %s's curValue or newValue should not be empty", node)
	}

	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	var put clientv3.Op
	if ttl <= 0 {
		put = clientv3.OpPut(node, newValue)
	} else {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), defaultOpTimeout)
		defer cancel()

		resp, err := w.client.Grant(timeoutCtx, ttl)
		if err != nil {
			return "", errors.Wrap(err, "")
		}

		put = clientv3.OpPut(node, newValue, clientv3.WithLease(resp.ID))
	}

	// leader会尝试保持自己的状态
	cmp := clientv3.Compare(clientv3.Value(node), "=", curValue)
	get := clientv3.OpGet(node)
	resp, err := w.client.Txn(timeoutCtx).If(cmp).Then(put).Else(get).Commit()
	if err != nil {
		return "", errors.Wrapf(err, "FAILED to swap node %s from %s to %s", node, curValue, newValue)
	}
	if resp.Succeeded {
		Logger.Printf("Successfully swap node %s from %s to %s", node, curValue, newValue)
		return "", nil
	}
	if resp.Responses[0].GetResponseRange().Count == 0 {
		return "", errors.Errorf("FAILED to swap node %s, node not exist, but want change value from %s to %s", node, curValue, newValue)
	}
	realValue := string(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	if realValue == newValue {
		Logger.Printf("FAILED to swap node %s, current value %s, but want change value from %s to %s", node, realValue, curValue, newValue)
		return realValue, errEtcdAlreadyExist
	}
	Logger.Printf("FAILED to swap node %s, current value %s, but want change value from %s to %s", node, realValue, curValue, newValue)
	return realValue, errEtcdValueNotMatch
}
