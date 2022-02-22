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
	"encoding/json"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// sm的任务: 管理governedService的container和shard监控
type shardTask struct {
	GovernedService string `json:"governedService"`
}

func (t *shardTask) String() string {
	b, _ := json.Marshal(t)
	return string(b)
}

func (t *shardTask) Validate() bool {
	return t.GovernedService != ""
}

type smShard struct {
	// smContainer 是真实的资源，包括：etcd client
	parent *smContainer

	// 通过sm的管理接口录入，业务可以自己指定
	id string

	// 说明当前shard属于哪个业务或者哪种user case
	service string

	lg *zap.Logger

	shardSpec *apputil.ShardSpec

	worker *Worker
}

func newShard(lg *zap.Logger, sc *smContainer, id string, spec *apputil.ShardSpec) (*smShard, error) {
	var st shardTask
	if err := json.Unmarshal([]byte(spec.Task), &st); err != nil {
		return nil, errors.Wrap(err, "")
	}

	s := smShard{
		parent:    sc,
		id:        id,
		lg:        lg,
		shardSpec: spec,
		service:   st.GovernedService,
	}

	var err error
	s.worker, err = newWorker(lg, s.parent, s.service)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func (s *smShard) Close() {
	s.worker.Close()

	s.lg.Info("closed",
		zap.String("id", s.id),
		zap.String("service", s.service),
		zap.Reflect("spec", s.shardSpec),
	)
}

func (s *smShard) Spec() *apputil.ShardSpec {
	return s.shardSpec
}

func (s *smShard) GetLoad() string {
	// TODO
	// 记录当前shard负责的工作单位时间内所需要的指令数量（程序的qps），多个shard的峰值qps叠加后可能导致cpu（这块我们只关注cpu）超出阈值，这种组合很多
	// 简单处理：不允许>=2的计算任务峰值qps导致的cpu负载超过我们设定的阈值，计算任务和cpu负载的关系要提前针对算法探测出来，这里的算法是指shard分配算法
	// 接入app本身也要参考这个提供load信息给sm，也可以根据自身情况抽象，例如：分布式计数器可以用每个shard的访问次数作为load，把cpu的问题抽象一下
	return "todo"
}
