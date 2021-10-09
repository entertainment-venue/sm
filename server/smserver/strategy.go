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
	"sort"

	"github.com/pkg/errors"
)

// 参考 https://github.com/entertainment-venue/LRMF/blob/main/assignor.go
func performAssignment(totalLen int, shards []string, cws containerWeightList) (map[string][]string, []string, error) {
	if cws.IsConflict(shards) {
		return nil, nil, errors.New("conflict err")
	}
	if totalLen < len(shards) || totalLen == 0 {
		return nil, nil, errors.New("len err")
	}

	plen := len(shards)
	clen := len(cws)
	if plen == 0 || clen == 0 {
		return nil, nil, nil
	}

	sort.Strings(shards)
	sort.Sort(cws)

	r := make(map[string][]string)

	var twice []string

	n := totalLen / clen
	m := totalLen % clen
	p := 0
	for i, cw := range cws {
		// 预计放到当前container的shard列表
		first := p
		last := first + n

		// 前几个container都多放一个shard
		if m > 0 && i < m {
			last++
		}

		// 应该放多少
		should := last - first

		// 配合上面对于last的处理，修正last的数量，用于之后的翻页
		if last > plen {
			last = plen
		}

		weight := len(cw.shards)
		if should <= weight {
			// cw承载的shard比预期的多，扩容时会发生，保留下来，等待二次分配
			diff := weight - should
			if diff > 0 {
				twice = append(twice, cw.shards[0:diff]...)
				cws[i].shards = cws[i].shards[diff:]
			}
			continue
		} else {
			idle := should - weight
			if idle == 0 {
				continue
			}
			end := first + idle
			if end < last {
				last = end
			}
		}

		for _, id := range shards[first:last] {
			r[cw.id] = append(r[cw.id], id)

			// 鑫的分配给到cws，方便twice二次分配
			cws[i].shards = append(cws[i].shards, id)
		}
		p = last
	}

	return r, twice, nil
}
