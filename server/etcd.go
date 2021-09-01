package server

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

type etcdWrapper struct {
	client *clientv3.Client

	ctr *container
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
	return &etcdWrapper{client: client, ctr: cr}, nil
}

func (w *etcdWrapper) nodePrefix(admin bool) string {
	if admin {
		return fmt.Sprintf("/bd/admin")
	} else {
		return fmt.Sprintf("/bd/app/%s", w.ctr.service)
	}
}

// /borderland/proxy/admin/leader
func (w *etcdWrapper) leaderNode() string {
	return fmt.Sprintf("%s/leader", w.nodePrefix(true))
}

func (w *etcdWrapper) nodeAppPrefix(service string) string {
	return fmt.Sprintf("/bd/app/%s", service)
}

func (w *etcdWrapper) nodeAppContainerHb(service string) string {
	return fmt.Sprintf("%s/containerhb/", w.nodeAppPrefix(service))
}

func (w *etcdWrapper) nodeAppContainerIdHb(service, id string) string {
	return fmt.Sprintf("%s/containerhb/%s", w.nodeAppPrefix(service), id)
}

// 存储分配当前关系
func (w *etcdWrapper) nodeAppShard(service string) string {
	return fmt.Sprintf("%s/shard/", w.nodeAppPrefix(service))
}

// /borderland/app/proxy/shard/业务自己定义的shard id
func (w *etcdWrapper) nodeAppShardId(service, id string) string {
	return fmt.Sprintf("%s/shard/%s", w.nodeAppPrefix(service), id)
}

func (w *etcdWrapper) nodeAppShardHb(service string) string {
	return fmt.Sprintf("%s/shardhb/", w.nodeAppPrefix(service))
}

func (w *etcdWrapper) nodeAppShardHbId(service, id string) string {
	return fmt.Sprintf("%s/shardhb/%s", w.nodeAppPrefix(service), id)
}

// /borderland/proxy/task
// 如果app的task节点存在任务，不能产生新的新的任务，必须等待ack完成
func (w *etcdWrapper) nodeAppTask(service string) string {
	return fmt.Sprintf("%s/task", w.nodeAppPrefix(service))
}

// /borderland/proxy/admin/containerhb/
func (w *etcdWrapper) nodeAppHbContainer(service string) string {
	return fmt.Sprintf("%s/containerhb/", w.nodeAppPrefix(service))
}

// /borderland/app/proxy/spec 存储app的基本信息
func (w *etcdWrapper) nodeAppSpec(service string) string {
	return fmt.Sprintf("%s/spec", w.nodeAppPrefix(service))
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

func (w *etcdWrapper) del(_ context.Context, prefix string) error {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.client.Delete(timeoutCtx, prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Deleted == 0 {
		Logger.Printf("FAILED to del %s no kv exist", prefix)
	}
	return nil
}

func (w *etcdWrapper) update(_ context.Context, key string, value string) error {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	_, err := w.client.Put(timeoutCtx, key, value)
	if err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (w *etcdWrapper) createAndGet(_ context.Context, nodes []string, values []string, leaseID clientv3.LeaseID) error {
	if len(nodes) == 0 {
		return errors.New("FAILED empty nodes")
	}

	mainNode := nodes[0]
	// 创建的场景下，cmp只发生一次
	cmp := clientv3.Compare(clientv3.CreateRevision(mainNode), "=", 0)

	var create []clientv3.Op
	for idx, node := range nodes {
		if leaseID == clientv3.NoLease {
			create = append(create, clientv3.OpPut(node, values[idx]))
		} else {
			create = append(create, clientv3.OpPut(node, values[idx], clientv3.WithLease(leaseID)))
		}
	}

	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.client.Txn(timeoutCtx).If(cmp).Then(create...).Commit()
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Succeeded {
		Logger.Printf("Successfully create node %+v with values %+v", nodes, values)
		return nil
	}
	// 创建失败，不需要再继续，业务认定自己是创建的场景，curValue不能走下面的compare and swap
	Logger.Printf("FAILED to create node %s with values %s because node already exist", nodes, values)
	return errEtcdNodeExist
}

func (w *etcdWrapper) compareAndSwap(_ context.Context, node string, curValue string, newValue string, ttl int64) (string, error) {
	if curValue == "" && newValue == "" {
		return "", errors.Errorf("FAILED node %s's curValue and newValue should not be empty both", node)
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
		return realValue, errEtcdValueExist
	}
	Logger.Printf("FAILED to swap node %s, current value %s, but want change value from %s to %s", node, realValue, curValue, newValue)
	return realValue, errEtcdValueNotMatch
}
