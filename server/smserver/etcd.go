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

// nodeManager 管理sm的etcd prefix
type nodeManager struct {
	smService string
}

// /sm/app/foo.bar
func (n *nodeManager) nodeSM() string {
	return apputil.EtcdPathAppPrefix(n.smService)
}

// /sm/app/foo.bar/leader
func (n *nodeManager) nodeSMLeader() string {
	return fmt.Sprintf("%s/leader", n.nodeSM())
}

// /sm/app/proxy.dev/
func (n *nodeManager) nodeService(service string) string {
	return apputil.EtcdPathAppPrefix(service) + "/"
}

// /sm/app/foo.bar/service/proxy.dev/spec
func (n *nodeManager) nodeServiceSpec(appService string) string {
	return fmt.Sprintf("%s/service/%s/spec", n.nodeSM(), appService)
}

// /sm/app/foo.bar/service/proxy.dev/shard/s1
func (n *nodeManager) nodeServiceShard(appService, shardId string) string {
	return fmt.Sprintf("%s/service/%s/shard/%s", n.nodeSM(), appService, shardId)
}

// /sm/app/proxy.dev/shardhb/
func (n *nodeManager) nodeServiceShardHb(appService string) string {
	return fmt.Sprintf("%s/shardhb/", apputil.EtcdPathAppPrefix(appService))
}

// /sm/app/proxy.dev/containerhb/
func (n *nodeManager) nodeServiceContainerHb(appService string) string {
	return fmt.Sprintf("%s/containerhb/", apputil.EtcdPathAppPrefix(appService))
}

// nodeServiceGuard /sm/app/proxy.dev/lease/guard
func (n *nodeManager) nodeServiceGuard(appService string) string {
	return fmt.Sprintf("%s/lease/guard", apputil.EtcdPathAppPrefix(appService))
}

// nodeServiceBridge /sm/app/proxy.dev/bridge
func (n *nodeManager) nodeServiceBridge(appService string) string {
	return fmt.Sprintf("%s/lease/bridge", apputil.EtcdPathAppPrefix(appService))
}

// nodeServiceWorkerGroup /sm/app/foo.bar/service/proxy.dev/workerpool
func (n *nodeManager) nodeServiceWorkerGroup(appService string) string {
	return fmt.Sprintf("%s/service/%s/workerpool", n.nodeSM(), appService)
}

// nodeServiceWorker /sm/app/foo.bar/service/proxy.dev/workerpool/workerGroup/worker
func (n *nodeManager) nodeServiceWorker(appService, workerGroup, worker string) string {
	return fmt.Sprintf("%s/%s/%s", n.nodeServiceWorkerGroup(appService), workerGroup, worker)
}
