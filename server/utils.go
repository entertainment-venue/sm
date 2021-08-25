package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

// 提出container和shard的公共属性
type admin struct {
	ctx    context.Context
	cancel context.CancelFunc

	id string

	// 在shard和container场景下是不通的含义
	service string

	// https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
	wg sync.WaitGroup
}

func tickerLoop(ctx context.Context, duration time.Duration, exitMsg string, fn func(ctx context.Context) error, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.Tick(duration)
	for {
		select {
		case <-ticker:
		case <-ctx.Done():
			Logger.Printf(exitMsg)
			return
		}
		if err := fn(ctx); err != nil {
			Logger.Printf("err: %v", err)
		}
	}
}

func watchLoop(ctx context.Context, ew *etcdWrapper, node string, exitMsg string, fn func(ctx context.Context, ev *clientv3.Event) error, wg *sync.WaitGroup) {
	defer wg.Done()

	var opts []clientv3.OpOption
	opts = append(opts, clientv3.WithPrefix())

watchLoop:
	wch := ew.client.Watch(ctx, node)
	for {
		var wr clientv3.WatchResponse
		select {
		case wr = <-wch:
		case <-ctx.Done():
			Logger.Printf(exitMsg)
			return
		}
		if err := wr.Err(); err != nil {
			Logger.Printf("err: %v", err)
			goto watchLoop
		}

		for _, ev := range wr.Events {
			if err := fn(ctx, ev); err != nil {
				Logger.Printf("err: %v", err)
				time.Sleep(defaultSleepTimeout)
				goto watchLoop
			}
		}
	}
}

func shardAllocateChecker(ctx context.Context, ew *etcdWrapper, hbContainerNode string, shardNode string, taskNode string) error {
	// 获取存活的container
	containerIdAndValue, err := ew.getKvs(ctx, hbContainerNode)
	if err != nil {
		return errors.Wrap(err, "")
	}
	var endpoints []string
	for containerId := range containerIdAndValue {
		endpoints = append(endpoints, containerId)
	}

	shardIdAndValue, err := ew.getKvs(ctx, shardNode)
	if err != nil {
		return errors.Wrap(err, "")
	}
	var (
		unassignedShards = make(map[string]struct{})
		allShards        []string
	)
	for id, value := range shardIdAndValue {
		var s shardSpec
		if err := json.Unmarshal([]byte(value), &s); err != nil {
			return errors.Wrap(err, "")
		}
		allShards = append(allShards, id)

		if s.ContainerId == "" {
			unassignedShards[id] = struct{}{}
		}

		var exist bool
		for endpoint := range containerIdAndValue {
			if endpoint == s.ContainerId {
				exist = true
				break
			}
		}
		if !exist {
			unassignedShards[id] = struct{}{}
		}
	}

	if len(unassignedShards) > 0 {
		Logger.Printf("got unassigned shards %v", unassignedShards)

		var actions moveActionList

		// leader做下数量层面的分配，提交到任务节点，会有operator来处理。
		// 此处是leader对于sm自己分片的监控，防止有shard(业务app)被漏掉。
		// TODO 会导致shard的大范围移动，可以让策略考虑这个问题
		containerIdAndShardIds := performAssignment(allShards, endpoints)
		for containerId, shardIds := range containerIdAndShardIds {
			for _, shardId := range shardIds {
				if _, ok := unassignedShards[shardId]; !ok {
					continue
				}
				actions = append(actions, &moveAction{
					ShardId:     shardId,
					AddEndpoint: containerId,
				})
			}
		}

		// 向自己的app任务节点发任务
		if _, err := ew.compareAndSwap(ctx, taskNode, "", actions.String(), -1); err != nil {
			return errors.Wrap(err, "")
		}
	}
	return nil
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Printf("get local IP failed, error is %+v\n", err)
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
