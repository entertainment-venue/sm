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
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type moveActionList []*moveAction

func (l *moveActionList) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

// 4 unit test
func (l moveActionList) Len() int { return len(l) }
func (l moveActionList) Less(i, j int) bool {
	return l[i].ShardId > l[j].ShardId
}
func (l moveActionList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type moveAction struct {
	Service      string `json:"service"`
	ShardId      string `json:"shardId"`
	DropEndpoint string `json:"dropEndpoint"`
	AddEndpoint  string `json:"addEndpoint"`

	// container场景下，leader的init操作可以放弃
	AllowDrop bool `json:"allowDrop"`
}

type Operator interface {
	Closer

	MoveLoop()
	Scale()
}

// container和shard上报两个维度的load，leader(sm)或者shard(app)探测到异常，会发布任务出来，operator就是这个任务的执行者
type operator struct {
	goroutineStopper

	service string

	ctr        *container
	httpClient *http.Client
	prevValue  string
}

func newOperator(cr *container, service string) (*operator, error) {
	op := operator{ctr: cr}
	op.ctx, op.cancel = context.WithCancel(context.Background())
	op.service = service

	op.httpClient = newHttpClient()

	go op.MoveLoop()

	// TODO scale

	return &op, nil
}

func (o *operator) Close() {
	o.cancel()
	o.wg.Wait()
	Logger.Printf("[operator] stopped for service %s", o.ctr.service)
}

// sm的shard需要能为接入app提供shard移动的能力，且保证每个任务被执行掉，所以任务会绑定在shard，防止sm的shard移动导致任务没人干
func (o *operator) MoveLoop() {
	fn := func(ctx context.Context, ev *clientv3.Event) error {
		if ev.Type == mvccpb.DELETE {
			return nil
		}

		if string(ev.Kv.Value) == o.prevValue {
			Logger.Printf("[operator] Duplicate event: %s", o.prevValue)
			return nil
		}

		if err := o.move(ev.Kv.Value); err != nil {
			return errors.Wrap(err, "")
		}

		return nil
	}

	key := o.ctr.ew.nodeAppTask(o.ctr.service)

	// Move只有对特定app负责的operator
	// 当前如果存在任务，直接开始执行
firstMove:
	resp, err := o.ctr.ew.get(o.ctx, key, []clientv3.OpOption{})
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

	watchLoop(o.ctx, o.ctr.ew, key, "[operator] service %s MoveLoop exit", fn, &o.wg)
}

// 保证at least once
func (o *operator) move(value []byte) error {
	var mal moveActionList
	if err := json.Unmarshal(value, &mal); err != nil {
		Logger.Printf("[operator] Unexpected err: %v", err)
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
			return o.dropAndAdd(ma)
		})
	}
	if err := g.Wait(); err != nil {
		Logger.Printf("err: %v", err)
		time.Sleep(defaultSleepTimeout)
		goto move
	}

	Logger.Printf("[operator] completed shard move task %s", string(value))

	// 利用etcd tx清空任务节点，任务节点已经空就停止
ack:
	key := o.ctr.ew.nodeAppTask(o.ctr.service)
	if _, err := o.ctr.ew.compareAndSwap(o.ctx, key, string(value), "", -1); err != nil {
		// 节点数据被破坏，需要人工介入
		Logger.Printf("err: %v", err)
		time.Sleep(defaultSleepTimeout)
		goto ack
	}

	return nil
}

func (o *operator) dropAndAdd(ma *moveAction) error {
	var (
		onlyAdd  bool
		onlyDrop bool
	)

	if ma.DropEndpoint != "" {
		if err := o.send(ma.ShardId, ma.DropEndpoint, "drop"); err != nil {
			return errors.Wrap(err, "")
		}
	} else {
		onlyAdd = true
	}

	if ma.AddEndpoint != "" {
		if err := o.send(ma.ShardId, ma.AddEndpoint, "add"); err != nil {
			if !ma.AllowDrop {
				return errors.Wrap(err, "")
			}

			Logger.Printf("[operator] FAILED to send move request %v, err: %v", *ma, err)

			// 只在leader竞选成功场景下会下发分配指令，没有Drop动作，这里允许放弃当前动作，后续有shardAllocateLoop和shardLoadLoop兜底
			// 如果下发失败，就必须去掉分配关系，以便shardAllocateLoop拿到的分配关系是比较真实的（这块即便下发都成功，shard可能因为异常停止工作）
			if err := o.remove(ma.ShardId, ma.Service); err != nil {
				return errors.Wrap(err, "")
			}
			return nil
		}
	} else {
		onlyDrop = true

		// 没有Add节点证明要把shard清除掉
		if err := o.ctr.ew.del(o.ctx, o.ctr.ew.nodeAppShardId(ma.Service, ma.ShardId)); err != nil {
			return errors.Wrap(err, "")
		}
	}

	Logger.Printf("[operator] Successfully move shard %s from %s to %s, onlyAdd: %b onlyDrop: %b", ma.ShardId, ma.DropEndpoint, ma.AddEndpoint, onlyAdd, onlyDrop)

	return nil
}

func (o *operator) send(id string, endpoint string, action string) error {
	param := make(map[string]string)
	param["shardId"] = id
	b, err := json.Marshal(param)
	if err != nil {
		return errors.Wrap(err, "")
	}

	urlStr := fmt.Sprintf("http://%s/borderland/container/%s-shard", endpoint, action)
	req, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewBuffer(b))
	if err != nil {
		return errors.Wrap(err, "")
	}

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("[operator] FAILED to %s shard %s, not 200", action, id)
	}
	return nil
}

func (o *operator) remove(id, service string) error {
	key := o.ctr.ew.nodeAppShardId(service, id)
	resp, err := o.ctr.ew.get(o.ctx, key, nil)
	if err != nil {
		return errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		Logger.Printf("[operator] Unexpected err, key %s not exist", key)
		return nil
	}

	var ss shardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &ss); err != nil {
		return errors.Wrap(err, "")
	}
	if ss.ContainerId == "" {
		return nil
	}
	ss.ContainerId = ""
	if _, err := o.ctr.ew.compareAndSwap(o.ctx, key, string(resp.Kvs[0].Value), ss.String(), -1); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (o *operator) Scale() {
	// TODO
	panic("unsupported Scale")
}
