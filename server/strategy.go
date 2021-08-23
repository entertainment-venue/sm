package main

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
