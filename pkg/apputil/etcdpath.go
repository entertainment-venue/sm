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

package apputil

import "fmt"

var (
	// etcdPrefix 需要可配置
	etcdPrefix = "/sm"
)

func InitEtcdPrefix(prefix string) {
	if prefix == "" {
		panic("prefix should not be empty")
	}
	etcdPrefix = prefix
}

func EtcdPathAppPrefix(service string) string {
	return fmt.Sprintf("%s/app/%s", etcdPrefix, service)
}

func EtcdPathAppContainerIdHb(service, id string) string {
	return fmt.Sprintf("%s/containerhb/%s", EtcdPathAppPrefix(service), id)
}

func EtcdPathAppShardHbId(service, id string) string {
	return fmt.Sprintf("%s/shardhb/%s", EtcdPathAppPrefix(service), id)
}

func EtcdPathAppShardId(service, id string) string {
	return fmt.Sprintf("%s/shard/%s", EtcdPathAppPrefix(service), id)
}
