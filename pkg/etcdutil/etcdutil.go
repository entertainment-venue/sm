// Copyright 2021 The entertainment-venue Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdutil

import (
	"context"
	"path/filepath"
	"strconv"
	"time"

	"github.com/entertainment-venue/sm/pkg/logutil"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	_ EtcdWrapper = new(EtcdClient)
)

var (
	defaultOpTimeout = 3 * time.Second
)

var (
	ErrEtcdNodeExist     = errors.New("etcd: node exist")
	ErrEtcdValueExist    = errors.New("etcd: value exist")
	ErrEtcdValueNotMatch = errors.New("etcd: value not match")
)

// EtcdWrapper 4 unit test
// etcd的方法已经是通过interface开放出来，这里进行二次封装
type EtcdWrapper interface {
	GetClient() *EtcdClient
	GetKV(_ context.Context, node string, opts []clientv3.OpOption) (*clientv3.GetResponse, error)
	GetKVs(ctx context.Context, prefix string) (map[string]string, error)
	UpdateKV(ctx context.Context, key string, value string) error
	DelKV(ctx context.Context, prefix string) error

	CreateAndGet(ctx context.Context, nodes []string, values []string, leaseID clientv3.LeaseID) error
	CompareAndSwap(_ context.Context, node string, curValue string, newValue string, leaseID clientv3.LeaseID) (string, error)
	Inc(_ context.Context, pfx string) (string, error)

	Ctx() context.Context
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

type EtcdClient struct {
	*clientv3.Client

	lg logutil.Logger
}

func (w *EtcdClient) GetClient() *EtcdClient {
	return w
}

func NewEtcdClient(endpoints []string, lg *zap.Logger) (*EtcdClient, error) {
	return NewEtcdClientWithCustomLogger(endpoints, logutil.NewZapLogger(lg))
}

func NewEtcdClientWithCustomLogger(endpoints []string, lg logutil.Logger) (*EtcdClient, error) {
	if len(endpoints) < 1 {
		return nil, errors.New("You must provide at least one etcd address")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second, DialOptions: []grpc.DialOption{grpc.WithBlock()}})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &EtcdClient{Client: client, lg: lg}, nil
}

func NewEtcdClientWithClient(client *clientv3.Client, lg *zap.Logger) *EtcdClient {
	return &EtcdClient{Client: client, lg: logutil.NewZapLogger(lg)}
}

func (w *EtcdClient) GetKV(_ context.Context, node string, opts []clientv3.OpOption) (*clientv3.GetResponse, error) {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.Get(timeoutCtx, node, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return resp, nil
}

func (w *EtcdClient) GetKVs(ctx context.Context, prefix string) (map[string]string, error) {
	// https://github.com/etcd-io/etcd/blob/master/tests/integration/clientv3/kv_test.go
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	resp, err := w.GetKV(ctx, prefix, opts)
	if err != nil {
		return nil, errors.Wrapf(err, "FAILED to GetKV prefix %s", prefix)
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

func (w *EtcdClient) DelKV(_ context.Context, prefix string) error {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	resp, err := w.Delete(timeoutCtx, prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Deleted == 0 {
		w.lg.Warn("no kv exist", zap.String("prefix", prefix))
	}
	return nil
}

func (w *EtcdClient) UpdateKV(_ context.Context, key string, value string) error {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	_, err := w.Put(timeoutCtx, key, value)
	if err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (w *EtcdClient) CreateAndGet(_ context.Context, nodes []string, values []string, leaseID clientv3.LeaseID) error {
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

	resp, err := w.Txn(timeoutCtx).If(cmp).Then(create...).Commit()
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Succeeded {
		w.lg.Info("create node success",
			zap.Strings("nodes", nodes),
			zap.Strings("values", values),
		)
		return nil
	}
	return ErrEtcdNodeExist
}

func (w *EtcdClient) CompareAndSwap(_ context.Context, node string, curValue string, newValue string, leaseID clientv3.LeaseID) (string, error) {
	if curValue == "" && newValue == "" {
		return "", errors.Errorf("FAILED node %s's curValue and newValue should not be empty both", node)
	}

	timeoutCtx, cancel := context.WithTimeout(context.TODO(), defaultOpTimeout)
	defer cancel()

	var put clientv3.Op
	if leaseID == clientv3.NoLease {
		put = clientv3.OpPut(node, newValue)
	} else {
		put = clientv3.OpPut(node, newValue, clientv3.WithLease(leaseID))
	}

	// leader会尝试保持自己的状态
	cmp := clientv3.Compare(clientv3.Value(node), "=", curValue)
	get := clientv3.OpGet(node)
	resp, err := w.Txn(timeoutCtx).If(cmp).Then(put).Else(get).Commit()
	if err != nil {
		return "", errors.Wrapf(err, "FAILED to swap node %s from %s to %s", node, curValue, newValue)
	}
	if resp.Succeeded {
		w.lg.Debug("swap node success",
			zap.String("node", node),
			zap.String("curValue", curValue),
			zap.String("newValue", newValue),
		)
		return "", nil
	}
	if resp.Responses[0].GetResponseRange().Count == 0 {
		return "", errors.Errorf("FAILED to swap node %s, node not exist, but want change value from %s to %s", node, curValue, newValue)
	}
	realValue := string(resp.Responses[0].GetResponseRange().Kvs[0].Value)
	if realValue == newValue {
		w.lg.Error("failed to swap node",
			zap.String("node", node),
			zap.String("realValue", realValue),
			zap.String("newValue", newValue),
			zap.Error(ErrEtcdNodeExist),
		)
		return realValue, ErrEtcdValueExist
	}
	w.lg.Warn("failed to swap node",
		zap.String("node", node),
		zap.String("etcd-value", realValue),
		zap.String("expect-value", curValue),
		zap.String("new-value", newValue),
		zap.Error(ErrEtcdValueNotMatch),
	)
	return realValue, ErrEtcdValueNotMatch
}

func (w *EtcdClient) Inc(_ context.Context, pfx string) (string, error) {
	if pfx == "" {
		return "", nil
	}
	gresp, err := w.GetKV(context.TODO(), pfx, nil)
	if err != nil {
		return "", err
	}
	if gresp.Count <= 0 {
		initValue := "1"
		if err := w.CreateAndGet(context.TODO(), []string{pfx}, []string{initValue}, clientv3.NoLease); err != nil {
			return "", err
		}
		return initValue, nil
	}

	cur, _ := strconv.ParseUint(string(gresp.Kvs[0].Value), 10, 64)
	curStr := strconv.FormatUint(cur, 10)
	newStr := strconv.FormatUint(cur+1, 10)
	if _, err := w.CompareAndSwap(context.TODO(), pfx, curStr, newStr, clientv3.NoLease); err != nil {
		return "", err
	}
	return newStr, nil
}
