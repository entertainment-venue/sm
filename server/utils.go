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

func shardAllocateChecker(ctx context.Context, ew *etcdWrapper, service string) error {
	// 获取存活的container
	containerIdAndValue, err := ew.getKvs(ctx, ew.nodeAppHbContainer(service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	var endpoints []string
	for containerId := range containerIdAndValue {
		endpoints = append(endpoints, containerId)
	}

	shardIdAndValue, err := ew.getKvs(ctx, ew.nodeAppShard(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	// 检查是否有shard没有在健康的container上
	var (
		gotUnassignedShards bool

		allShards   []string
		moveActions moveActionList

		// 方便下面对哪些shard有移动做判断
		curShardIdAndContainerId = make(map[string]string)

		// 用于下面针对container的判断
		shardContainerIds = make(map[string]struct{})
	)
	for id, value := range shardIdAndValue {
		var ss shardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}

		shardContainerIds[ss.ContainerId] = struct{}{}
		curShardIdAndContainerId[id] = ss.ContainerId

		if ss.Deleted {
			moveActions = append(moveActions, &moveAction{
				Service:      service,
				ShardId:      id,
				DropEndpoint: ss.ContainerId,
			})
		}

		allShards = append(allShards, id)

		if ss.ContainerId == "" {
			gotUnassignedShards = true
			break
		}

		var exist bool
		for endpoint := range containerIdAndValue {
			if endpoint == ss.ContainerId {
				exist = true
				break
			}
		}
		if !exist {
			gotUnassignedShards = true
			break
		}
	}

	// 检查是否有健康的container，没有被分配shard，也需要触发下面的performAssignment
	var gotUnassignedContainers bool
	for id := range containerIdAndValue {
		if _, ok := shardContainerIds[id]; !ok {
			gotUnassignedContainers = true
			break
		}
	}

	if gotUnassignedShards || gotUnassignedContainers {
		// leader做下数量层面的分配，提交到任务节点，会有operator来处理。
		// 此处是leader对于sm自己分片的监控，防止有shard(业务app)被漏掉。
		// TODO 会导致shard的大范围移动，可以让策略考虑这个问题
		newContainerIdAndShardIds := performAssignment(allShards, endpoints)

		for newId, shardIds := range newContainerIdAndShardIds {
			// 找出哪些shard是要变动的
			for _, shardId := range shardIds {
				curContainerId := curShardIdAndContainerId[shardId]
				if curContainerId == newId {
					continue
				}

				moveActions = append(moveActions, &moveAction{
					Service:      service,
					ShardId:      shardId,
					DropEndpoint: curContainerId,
					AddEndpoint:  newId,
				})
			}
		}
	}

	if len(moveActions) > 0 {
		// 向自己的app任务节点发任务
		if _, err := ew.compareAndSwap(ctx, ew.nodeAppTask(service), "", moveActions.String(), -1); err != nil {
			return errors.Wrap(err, "")
		}
	}
	return nil
}

func shardLoadChecker(ctx context.Context, eq *eventQueue, ev *clientv3.Event) error {
	if ev.IsCreate() {
		return nil
	}

	start := time.Now()
	qev := event{
		start: start.Unix(),
		load:  string(ev.Kv.Value),
	}

	if ev.IsModify() {
		qev.typ = evTypeShardUpdate
	} else {
		qev.typ = evTypeShardDel
		// 3s是给服务器container重启的事件
		qev.expect = start.Add(3 * time.Second).Unix()
	}
	eq.push(&qev)
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
