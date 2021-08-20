package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type moveActionList []*moveAction

type moveAction struct {
	ShardId      string `json:"shardId"`
	DropEndpoint string `json:"dropEndpoint"`
	AddEndpoint  string `json:"addEndpoint"`
}

type Operator interface {
	Closer

	Move()
	Scale()
}

// container和shard上报两个维度的load，leader(sm)或者shard(app)探测到异常，会发布任务出来，operator就是这个任务的执行者
type operator struct {
	admin

	cr         *container
	httpClient *http.Client
	prevValue  string
}

func newOperator(cr *container) (Operator, error) {
	op := operator{cr: cr}
	op.ctx, op.cancel = context.WithCancel(context.Background())

	httpDialContextFunc := (&net.Dialer{Timeout: 1 * time.Second, DualStack: true}).DialContext
	op.httpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: httpDialContextFunc,

			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 0,

			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 50,
		},
		Timeout: 3 * time.Second,
	}

	go op.Move()

	// TODO scale

	return &op, nil
}

func (o *operator) Close() {
	o.cancel()
	o.wg.Wait()
	Logger.Printf("operator exit for service %s stopped", c.cr.service)
}

// sm的shard需要能为接入app提供shard移动的能力，且保证每个任务被执行掉，所以任务会绑定在shard，防止sm的shard移动导致任务没人干
func (o *operator) Move() {
	fn := func(ctx context.Context, ev *clientv3.Event) error {
		if ev.Type == mvccpb.DELETE {
			return nil
		}

		if string(ev.Kv.Value) == o.prevValue {
			Logger.Printf("Duplicate event: %s", string(o.prevValue))
			return nil
		}

		if err := o.move(ev.Kv.Value); err != nil {
			return errors.Wrap(err, "")
		}

		return nil
	}

	// Move只有对特定app负责的operator
	// 当前如果存在任务，直接开始执行
firstMove:
	resp, err := o.cr.ew.get(o.ctx, o.cr.ew.appTaskNode(), []clientv3.OpOption{})
	if err != nil {
		Logger.Printf("err: %v", err)
		time.Sleep(defaultSleepTimeout)
		goto firstMove
	}
	if resp.Count > 0 {
		s := string(resp.Kvs[0].Value)
		if s != "" {
			if err := o.move(resp.Kvs[0].Value); err != nil {
				Logger.Printf("err: %v", err)
				time.Sleep(defaultSleepTimeout)
				goto firstMove
			}
		}
	}

	watchLoop(o.ctx, o.cr.ew, o.cr.ew.appTaskNode(), "moveLoop exit", fn, &o.wg)
}

// 保证at least once
func (o *operator) move(value []byte) error {
	var mal moveActionList
	if err := json.Unmarshal(value, &mal); err != nil {
		Logger.Printf("Unexpected err: %v", err)
		// return ASAP unmarshal失败重试没意义，需要人工接入进行数据修正
		return errors.Wrap(err, "")
	}

	// https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/
	// 单shard维度，先drop，再add，多个shard可以并行移动
move:
	g := new(errgroup.Group)
	for _, ma := range mal {
		ma := ma
		g.Go(func() error {
			if err := o.sendMoveRequest(ma.ShardId, ma.DropEndpoint, "drop"); err != nil {
				return errors.Wrap(err, "")
			}

			if err := o.sendMoveRequest(ma.ShardId, ma.AddEndpoint, "add"); err != nil {
				return errors.Wrap(err, "")
			}

			Logger.Printf("Successfully move shard %s from %s to %s", ma.ShardId, ma.DropEndpoint, ma.AddEndpoint)

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		Logger.Printf("err: %v", err)
		time.Sleep(defaultSleepTimeout)
		goto move
	}

	// 利用etcd tx清空任务节点，任务节点已经空就停止
ack:
	key := o.cr.ew.appTaskNode()
	if _, err := o.cr.ew.compareAndSwap(o.ctx, key, string(value), "", -1); err != nil {
		// 节点数据被破坏，需要人工介入
		Logger.Printf("err: %v", err)
		time.Sleep(defaultSleepTimeout)
		goto ack
	}

	return nil
}

func (o *operator) sendMoveRequest(id string, endpoint string, action string) error {
	dropParam := make(map[string]string)
	dropParam["shard_id"] = id
	b, err := json.Marshal(dropParam)
	if err != nil {
		return errors.Wrap(err, "")
	}

	urlStr := fmt.Sprintf("http://%s/borderland/shard/%s", endpoint, action)
	dropReq, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewBuffer(b))
	if err != nil {
		return errors.Wrap(err, "")
	}

	dropResp, err := o.httpClient.Do(dropReq)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer dropResp.Body.Close()
	ioutil.ReadAll(dropResp.Body)

	if dropResp.StatusCode != http.StatusOK {
		return errors.Errorf("Failed to %s shard %s, not 200", action, id)
	}
	return nil
}

func (o *operator) Scale() {
	// TODO
	panic("unsupported Scale")
}
