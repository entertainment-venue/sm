package server

import (
	"context"
	"encoding/json"

	"github.com/entertainment-venue/borderland/pkg/apputil"
	"github.com/pkg/errors"
)

// borderland的任务
type shardTask struct {
	GovernedService string `json:"governedService"`
}

type serverShard struct {
	stopper *apputil.GoroutineStopper

	// serverContainer 是真实的资源，etcd client、http client
	parent *serverContainer

	// 特定于sm业务的分片，service是sm集群的name
	id, service string

	mtWorker *maintenanceWorker
}

func startShard(_ context.Context, sc *serverContainer, id string, spec *apputil.ShardSpec) (*serverShard, error) {
	s := serverShard{parent: sc, id: id}

	var st shardTask
	if err := json.Unmarshal([]byte(spec.Task), &st); err != nil {
		return nil, errors.Wrap(err, "")
	}
	s.service = st.GovernedService

	// shard和op的数量相关
	if err := s.parent.NewOp(s.service); err != nil {
		return nil, errors.Wrap(err, "")
	}

	s.mtWorker = newMaintenanceWorker(s.parent, s.service)
	s.mtWorker.Start()

	return &s, nil
}

func (s *serverShard) Close() {
	// 关闭自己孩子的goroutine
	s.mtWorker.Close()

	// 关闭自己的
	s.stopper.Close()

	Logger.Printf("serverShard %s for service %s stopped", s.id, s.parent.service)
}

type shardLoad struct {
	RPS     int `json:"rps"`
	AvgTime int `json:"avgTime"`
}

func (s *shardLoad) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (s *serverShard) getLoad() string {
	// TODO shardLoad
	return ""
}
