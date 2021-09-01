package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
)

// 提出container和shard的公共属性
// 抽象数据结构，也会引入数据结构之间的耦合
type admin struct {
	// https://callistaenterprise.se/blogg/teknik/2019/10/05/go-worker-cancellation/
	// graceful close
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	id string

	// shard: 管理接入sm的业务app的shard，是被管理app的service的名称
	// leader: 管理sm集群内部的各shard，是sm集群的service的名称
	// container: 是所属app的service的名称
	service string
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

// 1 container 的增加/减少是优先级最高，目前可能涉及大量shard move
// 2 shard 被漏掉作为container检测的补充，最后校验，这种情况只涉及到漏掉的shard任务下发下去
func allocateChecker(ctx context.Context, ew *etcdWrapper, service string) error {
	// 获取当前的shard分配关系
	fixShardIdAndValue, err := ew.getKvs(ctx, ew.nodeAppShard(service))
	if err != nil {
		return errors.Wrap(err, "")
	}

	// 检查是否有shard没有在健康的container上
	var (
		// 与survive的container做比较，不一样需要做整体shard move
		// 1 多
		// 2 少
		// 3 不一样
		usingContainerIds          []string
		usingContainerIdMap        = make(map[string]struct{})
		usingShardIdAndContainerId = make(map[string]string)

		fixShardIds []string

		actions moveActionList
	)
	for id, value := range fixShardIdAndValue {
		fixShardIds = append(fixShardIds, id)

		var ss shardSpec
		if err := json.Unmarshal([]byte(value), &ss); err != nil {
			return errors.Wrap(err, "")
		}
		// 推迟需要删除的shard(在api接受指令)，如果是要删除的shard，所占资源不考虑
		if ss.Deleted {
			actions = append(actions, &moveAction{Service: service, ShardId: id, DropEndpoint: ss.ContainerId})
		}

		usingContainerIds = append(usingContainerIds, ss.ContainerId)
		usingContainerIdMap[ss.ContainerId] = struct{}{}
		usingShardIdAndContainerId[id] = ss.ContainerId
	}

	// 现有存活containers
	surviveContainerIdAndValue, err := ew.getKvs(ctx, ew.nodeAppHbContainer(service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	var surviveContainerIds []string
	for containerId := range surviveContainerIdAndValue {
		surviveContainerIds = append(surviveContainerIds, containerId)
	}

	var containerChanged bool
	sort.Strings(usingContainerIds)
	sort.Strings(surviveContainerIds)
	if !reflect.DeepEqual(usingContainerIds, surviveContainerIds) {
		containerChanged = true
	}
	if containerChanged {
		r := computeAndReallocate(service, fixShardIds, surviveContainerIds, usingShardIdAndContainerId)
		actions = append(actions, r...)
		if len(actions) > 0 {
			// 向自己的app任务节点发任务
			if _, err := ew.compareAndSwap(ctx, ew.nodeAppTask(service), "", actions.String(), -1); err != nil {
				return errors.Wrap(err, "")
			}
			Logger.Printf("Container changed for service %s, result %s", service, actions.String())
			return nil
		}
	}

	// container hb和固定分配关系一致，下面检查shard存活
	var shardChanged bool
	surviveShardIdAndValue, err := ew.getKvs(ctx, ew.nodeAppShardHb(service))
	if err != nil {
		return errors.Wrap(err, "")
	}
	surviveShardIdMap := make(map[string]struct{})
	for id := range surviveShardIdAndValue {
		surviveShardIdMap[id] = struct{}{}
	}
	for _, fixShardId := range fixShardIds {
		if _, ok := surviveShardIdMap[fixShardId]; !ok {
			shardChanged = true
			break
		}
	}

	if shardChanged {
		r := computeAndReallocate(service, fixShardIds, surviveContainerIds, usingShardIdAndContainerId)
		actions = append(actions, r...)
		if len(actions) > 0 {
			// 向自己的app任务节点发任务
			if _, err := ew.compareAndSwap(ctx, ew.nodeAppTask(service), "", actions.String(), -1); err != nil {
				return errors.Wrap(err, "")
			}
			Logger.Printf("Container changed for service %s, result %v", service, actions)
		}
	}

	return nil
}

func computeAndReallocate(service string, shards []string, containers []string, curShardIdAndContainerId map[string]string) moveActionList {
	var result moveActionList
	newContainerIdAndShardIds := performAssignment(shards, containers)
	for newId, shardIds := range newContainerIdAndShardIds {
		// 找出哪些shard是要变动的
		for _, shardId := range shardIds {
			curContainerId := curShardIdAndContainerId[shardId]
			if curContainerId == newId {
				continue
			}

			// FIXME curContainerId 可能不存在与containerIdAndValue，你给他发drop，可能也无法处理

			result = append(result, &moveAction{Service: service, ShardId: shardId, DropEndpoint: curContainerId, AddEndpoint: newId})
		}
	}
	return result
}

func shardLoadChecker(_ context.Context, eq *eventQueue, ev *clientv3.Event) error {
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

func containerLoadChecker(_ context.Context, eq *eventQueue, ev *clientv3.Event) error {
	if ev.IsCreate() {
		return nil
	}

	start := time.Now()
	qev := event{
		start: start.Unix(),
		load:  string(ev.Kv.Value),
	}

	if ev.IsModify() {
		qev.typ = evTypeContainerUpdate
	} else {
		qev.typ = evTypeContainerDel
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
