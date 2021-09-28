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
	"fmt"

	"github.com/entertainment-venue/sm/pkg/apputil"
)

// /sm/proxy/admin/leader
func nodeLeader(service string) string {
	return fmt.Sprintf("%s/leader", apputil.EtcdPathAppPrefix(service))
}

func nodeAppContainerHb(service string) string {
	return fmt.Sprintf("%s/containerhb/", apputil.EtcdPathAppPrefix(service))
}

// 存储分配当前关系
func nodeAppShard(service string) string {
	return fmt.Sprintf("%s/shard/", apputil.EtcdPathAppPrefix(service))
}

func nodeAppShardHb(service string) string {
	return fmt.Sprintf("%s/shardhb/", apputil.EtcdPathAppPrefix(service))
}

// /sm/proxy/admin/containerhb/
func nodeAppHbContainer(service string) string {
	return fmt.Sprintf("%s/containerhb/", apputil.EtcdPathAppPrefix(service))
}

// /sm/app/proxy/spec 存储app的基本信息
func nodeAppSpec(service string) string {
	return fmt.Sprintf("%s/spec", apputil.EtcdPathAppPrefix(service))
}
