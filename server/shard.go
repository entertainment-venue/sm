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
	"context"
	"encoding/json"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// sm的任务: 管理governedService的container和shard监控
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

	lg *zap.Logger

	shardSpec *apputil.ShardSpec
}

func startShard(ctx context.Context, lg *zap.Logger, sc *serverContainer, id string, spec *apputil.ShardSpec) (*serverShard, error) {
	s := serverShard{parent: sc, id: id, lg: lg, shardSpec: spec}

	var st shardTask
	if err := json.Unmarshal([]byte(spec.Task), &st); err != nil {
		return nil, errors.Wrap(err, "")
	}
	s.service = st.GovernedService

	// shard和op的数量相关
	if err := s.parent.NewOp(s.service); err != nil {
		return nil, errors.Wrap(err, "")
	}

	s.mtWorker = newMaintenanceWorker(ctx, lg, s.parent, s.service)
	s.mtWorker.Start()

	return &s, nil
}

func (s *serverShard) Close() {
	// 关闭自己孩子的goroutine
	s.mtWorker.Close()

	// 关闭自己的
	s.stopper.Close()

	s.lg.Info("serverShard exit",
		zap.String("id", s.id),
		zap.String("service", s.service),
	)
}

func (s *serverShard) Spec() *apputil.ShardSpec {
	return s.shardSpec
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
	return "todo"
}
