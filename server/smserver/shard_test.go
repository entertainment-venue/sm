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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

func Test_shardTask(t *testing.T) {
	task := &shardTask{GovernedService: ""}
	if task.Validate() {
		t.Error("Validate should be false")
		t.SkipNow()
	}
}

func TestShard(t *testing.T) {
	suite.Run(t, new(ShardTestSuite))
}

type ShardTestSuite struct {
	suite.Suite

	shard *smShard
}

func (suite *ShardTestSuite) SetupTest() {
	lg, _ := zap.NewDevelopment()

	shard := &smShard{
		lg: lg,

		service: mock.Anything,
	}
	suite.shard = shard
}

func (suite *ShardTestSuite) TestChanged() {
	var tests = []struct {
		a      []string
		b      []string
		expect bool
	}{
		{
			a:      []string{},
			b:      []string{},
			expect: false,
		},
		{
			a:      []string{"1"},
			b:      []string{"1"},
			expect: false,
		},
		{
			a:      []string{"1"},
			b:      []string{},
			expect: true,
		},
		{
			a:      []string{"1", "2"},
			b:      []string{"2", "1"},
			expect: false,
		},
		{
			a:      []string{"1", "2"},
			b:      []string{"1", "2", "3"},
			expect: true,
		},
	}
	for _, tt := range tests {
		r := suite.shard.changed(tt.a, tt.b)
		assert.Equal(suite.T(), r, tt.expect)
	}
}

func (suite *ShardTestSuite) TestRB() {
	var tests = []struct {
		fixShardIdAndManualContainerId ArmorMap
		hbContainerIdAndAny            ArmorMap
		hbShardIdAndContainerId        ArmorMap
		expect                         moveActionList
	}{
		// container新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
				"s3": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s3", AddEndpoint: "c2"},
			},
		},

		// container新增
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c1",
			},
			expect: moveActionList{
				&moveAction{Service: suite.shard.service, ShardId: "s1", DropEndpoint: "c1", AddEndpoint: "c2"},
			},
		},

		// container存活，没有shard需要移动
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c2",
			},
			expect: nil,
		},
		// container存活，没有shard需要移动，和顺序无关
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c2",
				"s2": "c1",
			},
			expect: nil,
		},
		// container存活，没有shard需要移动，和顺序无关
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c1": "",
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "",
			},
			expect: nil,
		},

		// container不存活，数据不一致不处理
		{
			fixShardIdAndManualContainerId: ArmorMap{
				"s1": "",
				"s2": "",
			},
			hbContainerIdAndAny: ArmorMap{
				"c2": "",
			},
			hbShardIdAndContainerId: ArmorMap{
				"s1": "c1",
				"s2": "c2",
			},
			expect: nil,
		},
	}

	for _, tt := range tests {
		r := suite.shard.extractShardMoves(tt.fixShardIdAndManualContainerId, tt.hbContainerIdAndAny, tt.hbShardIdAndContainerId, nil)
		assert.Equal(suite.T(), r, tt.expect)
	}
}
