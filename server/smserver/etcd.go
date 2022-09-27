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
	"path"
	"strings"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
)

// nodeManager 管理sm的etcd prefix
type nodeManager struct {
	smService string
}

// SMRootPath /sm/app/foo.bar
func (n *nodeManager) SMRootPath() string {
	return etcdutil.ServicePath(n.smService)
}

// LeaderPath /sm/app/foo.bar/leader
func (n *nodeManager) LeaderPath() string {
	return path.Join(n.SMRootPath(), "leader")
}

// ServicePath sm会需要得到外部service的路径
func (n *nodeManager) ServicePath(service string) string {
	return path.Join(n.SMRootPath(), "service", service)
}

// ServiceSpecPath /sm/app/foo.bar/service/proxy.dev/spec
func (n *nodeManager) ServiceSpecPath(appService string) string {
	return path.Join(n.ServicePath(appService), "spec")
}

// ShardPath /sm/app/foo.bar/service/proxy.dev/shard/s1
func (n *nodeManager) ShardPath(appService, shardId string) string {
	if shardId == "" {
		panic("shardId should not empty")
	}
	return path.Join(n.ServicePath(appService), "shard", shardId)
}

// ShardDir /sm/app/foo.bar/service/proxy.dev/shard/
func (n *nodeManager) ShardDir(appService string) string {
	return path.Join(n.ServicePath(appService), "shard") + "/"
}

// WorkerGroupPath /sm/app/foo.bar/service/proxy.dev/workerpool
func (n *nodeManager) WorkerGroupPath(appService string) string {
	return path.Join(n.ServicePath(appService), "workerpool")
}

// WorkerPath /sm/app/foo.bar/service/proxy.dev/workerpool/workerGroup/worker
func (n *nodeManager) WorkerPath(appService, workerGroup, worker string) string {
	return path.Join(n.WorkerGroupPath(appService), workerGroup, worker)
}

// ExternalServiceDir /sm/app/proxy.dev/
func (n *nodeManager) ExternalServiceDir(service string) string {
	return etcdutil.ServicePath(service) + "/"
}

// ExternalShardHbDir /sm/app/proxy.dev/shardhb/
func (n *nodeManager) ExternalShardHbDir(appService string) string {
	return path.Join(etcdutil.ServicePath(appService), "shardhb") + "/"
}

// ExternalContainerHbDir /sm/app/proxy.dev/containerhb/
func (n *nodeManager) ExternalContainerHbDir(appService string) string {
	return path.Join(etcdutil.ServicePath(appService), "containerhb") + "/"
}

// ExternalLeaseGuardPath /sm/app/proxy.dev/lease/guard
func (n *nodeManager) ExternalLeaseGuardPath(appService string) string {
	return etcdutil.LeaseGuardPath(appService)
}

// ExternalLeaseBridgePath /sm/app/proxy.dev/bridge
func (n *nodeManager) ExternalLeaseBridgePath(appService string) string {
	return etcdutil.LeaseBridgePath(appService)
}

// parseWorkerGroupAndContainer /sm/app/foo.bar/service/foo.bar/workerpool/g1/127.0.0.1:8801
func (n *nodeManager) parseWorkerGroupAndContainer(path string) (string, string) {
	arr := strings.Split(path, "/")
	if len(arr) >= 2 {
		return arr[len(arr)-2], arr[len(arr)-1]
	}
	return "", ""
}

// parseContainer /sm/app/foo.bar/containerhb/127.0.0.1:8801/694d818416078d06
func (n *nodeManager) parseContainer(hbPath string) string {
	arr := strings.Split(hbPath, "/")
	if len(arr) >= 2 {
		return arr[len(arr)-2]
	}
	return ""
}
