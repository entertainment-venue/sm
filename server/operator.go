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

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type moveAction struct {
	Service      string `json:"service"`
	ShardId      string `json:"shardId"`
	DropEndpoint string `json:"dropEndpoint"`
	AddEndpoint  string `json:"addEndpoint"`

	// container场景下，leader的init操作可以放弃
	AllowDrop bool `json:"allowDrop"`
}

func (action *moveAction) String() string {
	b, _ := json.Marshal(action)
	return string(b)
}

type moveActionList []*moveAction

func (l *moveActionList) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

// 4 unit test
func (l moveActionList) Len() int { return len(l) }
func (l moveActionList) Less(i, j int) bool {
	return l[i].ShardId < l[j].ShardId
}
func (l moveActionList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// container和shard上报两个维度的load，leader(sm)或者shard(app)探测到异常，会发布任务出来，operator就是这个任务的执行者
type operator struct {
	lg      *zap.Logger
	parent  *serverContainer
	service string

	stopper *apputil.GoroutineStopper
	hc      *http.Client
}

func newOperator(lg *zap.Logger, sc *serverContainer, service string) (*operator, error) {
	op := operator{
		lg:      lg,
		parent:  sc,
		service: service,

		stopper: &apputil.GoroutineStopper{},
		hc:      newHttpClient(),
	}

	op.stopper.Wrap(
		func(ctx context.Context) {
			op.moveLoop(ctx)
		})

	// TODO scale

	return &op, nil
}

func (o *operator) Close() {
	if o.stopper != nil {
		o.stopper.Close()
	}
	o.lg.Info("operator closed", zap.String("service", o.service))
}

func (o *operator) moveLoop(ctx context.Context) {
	key := apputil.EtcdPathAppShardTask(o.service)

	// Move只有对特定app负责的operator
	// 当前如果存在任务，直接开始执行
dealWithLatestTask:
	resp, err := o.parent.Client.GetKV(ctx, key, []clientv3.OpOption{})
	if err != nil {
		o.lg.Error("failed to GetKV",
			zap.String("key", key),
			zap.Error(err),
		)
		time.Sleep(defaultSleepTimeout)
		goto dealWithLatestTask
	}
	if resp.Count > 0 && string(resp.Kvs[0].Value) != "" {
		o.lg.Info("got task",
			zap.String("service", o.service),
			zap.String("value", string(resp.Kvs[0].Value)),
		)
		if err := o.move(ctx, resp.Kvs[0].Value); err != nil {
			o.lg.Error("failed to move",
				zap.Error(err),
				zap.ByteString("value", resp.Kvs[0].Value),
			)

			time.Sleep(defaultSleepTimeout)
			goto dealWithLatestTask
		}
	} else {
		o.lg.Info("empty shard move task",
			zap.String("key", key),
			zap.String("service", o.service),
		)
	}

	fn := func(ctx context.Context, ev *clientv3.Event) error {
		if ev.Type == mvccpb.DELETE {
			o.lg.Error("unexpected event", zap.Reflect("ev", ev))
			return nil
		}

		// 任务被清空，会出发一次时间
		if string(ev.Kv.Value) == "" {
			o.lg.Warn("got task erased event", zap.String("service", o.service))
			return nil
		}

		// 不接受重复的任务，在一个任务运行时，eq提交任务也会失败
		if ev.PrevKv != nil && string(ev.Kv.Value) == string(ev.PrevKv.Value) {
			o.lg.Warn("duplicate event", zap.ByteString("prevValue", ev.PrevKv.Value))
			return nil
		}

		if err := o.move(ctx, ev.Kv.Value); err != nil {
			return errors.Wrap(err, "")
		}
		return nil
	}

	apputil.WatchLoop(ctx, o.lg, o.parent.Client.Client, key, "[operator] service %s moveLoop exit", fn)
}

// 保证at least once
func (o *operator) move(ctx context.Context, value []byte) error {
	var mal moveActionList
	if err := json.Unmarshal(value, &mal); err != nil {
		o.lg.Error("failed to unmarshal",
			zap.ByteString("value", value),
			zap.Error(err),
		)
		// return ASAP unmarshal失败重试没意义，需要人工接入进行数据修正
		return errors.Wrap(err, "")
	}
	o.lg.Info("receive move action list", zap.Reflect("mal", mal))

	// https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/
	// 单shard维度，先drop，再add，多个shard可以并行移动
move:
	g := new(errgroup.Group)
	for _, ma := range mal {
		ma := ma
		g.Go(func() error {
			return o.dropOrAdd(ctx, ma)
		})
	}
	if err := g.Wait(); err != nil {
		o.lg.Error("dropOrAdd err", zap.Error(err))
		time.Sleep(defaultSleepTimeout)
		goto move
	}

	o.lg.Info("complete move", zap.ByteString("value", value))

	// 利用etcd tx清空任务节点，任务节点已经空就停止
ack:
	taskKey := apputil.EtcdPathAppShardTask(o.service)
	if _, err := o.parent.Client.CompareAndSwap(ctx, taskKey, string(value), "", -1); err != nil {
		// 节点数据被破坏，需要人工介入
		o.lg.Error("failed to CompareAndSwap",
			zap.Error(err),
			zap.String("key", taskKey),
			zap.ByteString("value", value),
		)
		time.Sleep(defaultSleepTimeout)
		goto ack
	}
	o.lg.Info("remove task",
		zap.String("key", taskKey),
		zap.String("value", string(value)),
	)

	return nil
}

func (o *operator) dropOrAdd(ctx context.Context, ma *moveAction) error {
	var (
		onlyAdd  bool
		onlyDrop bool
	)

	if ma.DropEndpoint != "" {
		if err := o.send(ctx, ma.ShardId, ma.DropEndpoint, "drop"); err != nil {
			return errors.Wrap(err, "")
		}
	} else {
		onlyAdd = true
	}

	if ma.AddEndpoint != "" {
		if err := o.send(ctx, ma.ShardId, ma.AddEndpoint, "add"); err != nil {
			if !ma.AllowDrop {
				return errors.Wrap(err, "")
			}

			o.lg.Error("failed to send",
				zap.Error(err),
				zap.Reflect("value", ma),
			)

			// 只在leader竞选成功场景下会下发分配指令，没有Drop动作，这里允许放弃当前动作，后续有shardAllocateLoop和shardLoadLoop兜底
			// 如果下发失败，就必须去掉分配关系，以便shardAllocateLoop拿到的分配关系是比较真实的（这块即便下发都成功，shard可能因为异常停止工作）
			if err := o.remove(ctx, ma.ShardId, ma.Service); err != nil {
				return errors.Wrap(err, "")
			}
			return nil
		}

	} else {
		onlyDrop = true

		// 没有Add节点证明要把shard清除掉
		if err := o.parent.Client.DelKV(ctx, apputil.EtcdPathAppShardId(ma.Service, ma.ShardId)); err != nil {
			return errors.Wrap(err, "")
		}
	}
	o.lg.Info("move shard request success",
		zap.Reflect("ma", ma),
		zap.Bool("onlyAdd", onlyAdd),
		zap.Bool("onlyDrop", onlyDrop),
	)

	// 标记shard分配信息到shard节点
	shardKey := apputil.EtcdPathAppShardId(ma.Service, ma.ShardId)
	resp, err := o.parent.Client.GetKV(ctx, shardKey, nil)
	if err != nil {
		return errors.Wrap(err, "")
	}
	var ss apputil.ShardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &ss); err != nil {
		return errors.Wrap(err, "")
	}
	ss.ContainerId = o.parent.id
	value := ss.String()
	if _, err := o.parent.Client.CompareAndSwap(ctx, shardKey, string(resp.Kvs[0].Value), value, -1); err != nil {
		return errors.Wrap(err, "")
	}
	o.lg.Info("move shard etcd CompareAndSwap success",
		zap.String("key", shardKey),
		zap.String("cur", string(resp.Kvs[0].Value)),
		zap.String("new", value),
	)

	return nil
}

func (o *operator) send(_ context.Context, id string, endpoint string, action string) error {
	msg := apputil.ShardOpMessage{Id: id}
	b, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "")
	}

	urlStr := fmt.Sprintf("http://%s/sm/admin/%s-shard", endpoint, action)
	req, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewBuffer(b))
	if err != nil {
		return errors.Wrap(err, "")
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := o.hc.Do(req)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer resp.Body.Close()
	rb, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("[operator] FAILED to %s move shard %s, not 200", action, id)
	}

	o.lg.Info("send success",
		zap.String("urlStr", urlStr),
		zap.String("action", action),
		zap.String("response", string(rb)),
	)
	return nil
}

func (o *operator) remove(ctx context.Context, id, service string) error {
	key := apputil.EtcdPathAppShardId(service, id)
	resp, err := o.parent.Client.GetKV(ctx, key, nil)
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		o.lg.Warn("failed to remove, key not exist", zap.String("key", key))
		return nil
	}

	var ss apputil.ShardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &ss); err != nil {
		return errors.Wrap(err, "")
	}
	if ss.ContainerId == "" {
		o.lg.Info("container already removed", zap.String("key", key))
		return nil
	}
	ss.ContainerId = ""

	value := ss.String()
	if _, err := o.parent.Client.CompareAndSwap(ctx, key, string(resp.Kvs[0].Value), value, -1); err != nil {
		return errors.Wrap(err, "")
	}
	o.lg.Info("container removed",
		zap.String("key", key),
		zap.String("value", value),
	)
	return nil
}

func (o *operator) Scale() {
	// TODO
	panic("unsupported Scale")
}
