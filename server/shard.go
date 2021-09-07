package server

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
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

	// 软删除：borderland的shard会检查shard是否都分配给了健康的container，已经存在goroutine，直接在这个goroutine中处理删除
	Deleted bool `json:"deleted"`
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

// shard需要实现该接口，帮助理解程序设计，不会有app实现多种doer
type Sharder interface {
	Closer

	// 作为被管理的shard的角色，上传load
	LoadUploader
}

type shard struct {
	goroutineStopper

	id string

	// 被管理app的service name
	service string

	// container 是真实的资源，etcd client、http client
	ctr *container

	// shard 保活手段和 container 分开，把load的上传和lease绑定起来，两方面
	// 1 自己周期hb，机制没有session中的keepalive健全
	// 2 shard 掉线后，load数据自动清理
	session *concurrency.Session

	mw MaintenanceWorker

	stat *shardStat
}

func newShard(id string, ctr *container) (*shard, error) {
	s := shard{ctr: ctr}
	s.id = id
	s.ctx, s.cancel = context.WithCancel(context.Background())

	var err error
	s.session, err = concurrency.NewSession(s.ctr.ew.client, concurrency.WithTTL(defaultSessionTimeout))
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
	resp, err := s.ctr.ew.get(s.ctx, s.ctr.ew.nodeAppShardId(s.ctr.service, s.id), nil)
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
	s.service = task.GovernedService

	// shard和op的数量相关
	if err := s.ctr.TryNewOp(s.service); err != nil {
		return nil, errors.Wrap(err, "")
	}

	s.mw = newMaintenanceWorker(s.ctr, s.service)

	s.wg.Add(1)
	go s.UploadLoad()

	// 检查app的分配和app shard上传的负载是否健康
	s.mw.Start()

	return &s, nil
}

func (s *shard) Close() {
	s.mw.Close()

	s.cancel()
	s.wg.Wait()
	Logger.Printf("shard %s for service %s stopped", s.id, s.ctr.service)
}

type shardStat struct {
	RPS     int `json:"rps"`
	AvgTime int `json:"avgTime"`
}

func (s *shardStat) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (s *shard) UploadLoad() {
	defer s.wg.Done()

	fn := func(ctx context.Context) error {
		ss := shardStat{}

		// 参考etcd clientv3库中的election.go，把负载数据与lease绑定在一起，并利用session.go做liveness保持
		k := s.ctr.ew.nodeAppShardHbId(s.ctr.service, s.id)
		if _, err := s.ctr.ew.client.Put(s.ctx, k, ss.String(), clientv3.WithLease(s.session.Lease())); err != nil {
			return errors.Wrap(err, "")
		}
		return nil
	}

	tickerLoop(s.ctx, defaultShardLoopInterval, "heartbeat exit", fn, &s.wg)
}
