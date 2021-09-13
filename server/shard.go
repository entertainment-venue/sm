package server

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/entertainment-venue/borderland/pkg/apputil"
	"github.com/pkg/errors"
)

type shardSpec struct {
	// 存储下，方便开发
	Service string `json:"service"`

	// 任务内容
	Task string `json:"task"`

	UpdateTime int64 `json:"updateTime"`

	// 所属container，在任务分配场景设置
	ContainerId string `json:"containerId"`

	// TODO 提供后台移动shard的能力，但是该能力开发较复杂，暂时不提供
	// 软删除：borderland的shard会检查shard是否都分配给了健康的container，已经存在goroutine，直接在这个goroutine中处理删除
	// Deleted bool `json:"deleted"`
}

// borderland的任务
type shardTask struct {
	GovernedService string `json:"governedService"`
}

func (s *shardSpec) parseTask() (*shardTask, error) {
	var st shardTask
	if err := json.Unmarshal([]byte(s.Task), &st); err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &st, nil
}

func (s *shardSpec) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type shardLoad struct {
	RPS     int `json:"rps"`
	AvgTime int `json:"avgTime"`
}

func (s *shardLoad) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type shard struct {
	stopper *apputil.GoroutineStopper

	// 特定于sm业务的分片，service是sm集群的name
	id, service string

	// serverContainer 是真实的资源，etcd client、http client
	sc *serverContainer

	// shard 保活手段和 serverContainer 分开，把load的上传和lease绑定起来，两方面
	// 1 自己周期hb，机制没有session中的keepalive健全
	// 2 shard 掉线后，load数据自动清理
	session *concurrency.Session

	mtWorker *maintenanceWorker

	stat *shardLoad
}

func startShard(ctx context.Context, id string, sc *serverContainer) (*shard, error) {
	s := shard{sc: sc}

	var err error
	s.session, err = concurrency.NewSession(s.sc.Client.Client, concurrency.WithTTL(defaultSessionTimeout))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	// 保持shard级别session的重试
	go func() {
		defer s.Close()
		for range s.session.Done() {

		}
		Logger.Printf("shard %s session closed", s.id)
	}()

	// 获取shard的任务信息，在sm场景下，shard中包含所负责的app的service信息
	resp, err := s.sc.Client.GetKV(ctx, s.sc.ew.nodeAppShardId(s.sc.service, s.id), nil)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if resp.Count == 0 {
		err = errors.Errorf("Failed to get shard %s content", s.id)
		return nil, errors.Wrap(err, "")
	}

	// 取出任务详情
	var ss shardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &ss); err != nil {
		return nil, errors.Wrap(err, "")
	}
	task, err := ss.parseTask()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	s.id = id
	s.service = task.GovernedService

	// shard和op的数量相关
	if err := s.sc.NewOp(s.service); err != nil {
		return nil, errors.Wrap(err, "")
	}

	s.mtWorker = newMaintenanceWorker(s.sc, s.service)

	s.stopper.Wrap(
		func(ctx context.Context) {
			fn := func(ctx context.Context) error {
				ss := shardLoad{}

				// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持
				k := s.sc.ew.nodeAppShardHbId(s.sc.service, s.id)
				if _, err := s.sc.Client.Put(ctx, k, ss.String(), clientv3.WithLease(s.session.Lease())); err != nil {
					return errors.Wrap(err, "")
				}
				return nil
			}

			apputil.TickerLoop(ctx, defaultShardLoopInterval, "heartbeat exit", fn)
		},
	)

	// 检查app的分配和app shard上传的负载是否健康
	s.mtWorker.Start()

	return &s, nil
}

func (s *shard) Close() {
	// 关闭自己孩子的goroutine
	s.mtWorker.Close()

	// 关闭自己的
	s.stopper.Close()

	Logger.Printf("shard %s for service %s stopped", s.id, s.sc.service)
}
