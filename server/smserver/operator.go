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

package smserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
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

	// 方便追溯每一个ma的执行结果和轨迹，方便追查问题
	TraceId string `json:"traceId"`
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

// Len 4 unit test
func (l moveActionList) Len() int { return len(l) }
func (l moveActionList) Less(i, j int) bool {
	return l[i].ShardId < l[j].ShardId
}
func (l moveActionList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// container和shard上报两个维度的load，leader(sm)或者shard(app)探测到异常，会发布任务出来，operator就是这个任务的执行者
type operator struct {
	parent *smContainer
	lg     *zap.Logger

	// operator 属于接入业务的service
	service string

	gs *apputil.GoroutineStopper
	hc *http.Client
}

func newOperator(lg *zap.Logger, sc *smContainer, service string) *operator {
	op := operator{
		lg:      lg,
		parent:  sc,
		service: service,

		gs: &apputil.GoroutineStopper{},
		hc: newHttpClient(),
	}

	lg.Info(
		"operator started",
		zap.String("service", service),
	)

	return &op
}

func (o *operator) Close() {
	if o.gs != nil {
		o.gs.Close()
	}
	o.lg.Info("operator closed", zap.String("service", o.service))
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

	var (
		// 增加重试机制
		retry   = 1
		counter = 0
		succ    bool
	)
	for counter <= retry {
		if counter > 0 {
			time.Sleep(defaultSleepTimeout)
		}

		g := new(errgroup.Group)
		for _, ma := range mal {
			ma := ma
			g.Go(func() error {
				return o.dropOrAdd(ctx, ma)
			})
		}
		if err := g.Wait(); err != nil {
			o.lg.Error("dropOrAdd err", zap.Error(err))
			counter++
		} else {
			succ = true
			break
		}
	}

	valueStr := string(value)

	o.lg.Info(
		"complete move",
		zap.Bool("succ", succ),
		zap.String("value", valueStr),
	)

	// 利用etcd tx清空任务节点，任务节点已经空就停止
	taskKey := apputil.EtcdPathAppShardTask(o.service)
	if _, err := o.parent.Client.CompareAndSwap(ctx, taskKey, valueStr, "", clientv3.NoLease); err != nil {
		// 节点数据被破坏，需要人工介入
		o.lg.Error(
			"CompareAndSwap error",
			zap.String("key", taskKey),
			zap.String("value", valueStr),
			zap.Error(err),
		)
		// 这块不能重试，可能会因为网络问题(例如：etcd更新成功，但是返回err)卡在CompareAndSwap
	} else {
		o.lg.Info(
			"CompareAndSwap succ",
			zap.String("key", taskKey),
			zap.String("value", valueStr),
		)
	}

	return nil
}

func (o *operator) dropOrAdd(ctx context.Context, ma *moveAction) error {
	var (
		onlyAdd  bool
		onlyDrop bool
	)

	if ma.DropEndpoint != "" {
		if err := o.send(ctx, ma.ShardId, ma.DropEndpoint, "drop", ma.TraceId); err != nil {
			return errors.Wrap(err, "")
		}
	} else {
		onlyAdd = true
	}

	if ma.AddEndpoint != "" {
		if err := o.send(ctx, ma.ShardId, ma.AddEndpoint, "add", ma.TraceId); err != nil {
			if !ma.AllowDrop {
				return errors.Wrap(err, "")
			}

			o.lg.Error("failed to add",
				zap.Error(err),
				zap.Reflect("value", ma),
			)
			return nil
		}

	} else {
		onlyDrop = true

		// 没有Add节点证明要把shard清除掉
		// if err := o.container.Client.DelKV(ctx, apputil.EtcdPathAppShardId(ma.Service, ma.ShardId)); err != nil {
		// 	return errors.Wrap(err, "")
		// }
	}
	o.lg.Info("move shard request success",
		zap.Reflect("ma", ma),
		zap.Bool("onlyAdd", onlyAdd),
		zap.Bool("onlyDrop", onlyDrop),
	)
	return nil
}

func (o *operator) send(_ context.Context, id string, endpoint string, action string, traceId string) error {
	msg := apputil.ShardOpMessage{Id: id, TraceId: traceId}
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

	o.lg.Info("http request success",
		zap.String("urlStr", urlStr),
		zap.ByteString("request", b),
		zap.ByteString("response", rb),
	)
	return nil
}

func (o *operator) Scale() {
	// TODO
	panic("unsupported Scale")
}
