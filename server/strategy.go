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
	"sort"
)

// 参考 https://github.com/entertainment-venue/LRMF/blob/main/assignor.go
func performAssignment(shardIds []string, instanceIds []string) map[string][]string {
	result := make(map[string][]string)

	plen := len(shardIds)
	clen := len(instanceIds)
	if plen == 0 || clen == 0 {
		return result
	}

	sort.Strings(shardIds)
	sort.Strings(instanceIds)

	// debug
	instanceIdAndTks := make(map[string][]string)

	n := plen / clen
	m := plen % clen
	p := 0
	for i, instanceId := range instanceIds {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > plen {
			last = plen
		}

		for _, id := range shardIds[first:last] {
			result[instanceId] = append(result[instanceId], id)

			// debug
			instanceIdAndTks[instanceId] = append(instanceIdAndTks[instanceId], id)
		}
		p = last
	}

	// debug
	var ids []string
	for _, id := range shardIds {
		ids = append(ids, id)
	}

	Logger.Printf("Divided: \n shardIds => (%+v) \n instanceId => (%+v) \n result => (%+v) \n", ids, instanceIds, result)

	return result
}
