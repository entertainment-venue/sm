package server

import (
	"fmt"

	"github.com/entertainment-venue/borderland/pkg/apputil"
)

type etcdWrapper struct {
}

func newEtcdWrapper() *etcdWrapper {
	return &etcdWrapper{}
}

// /borderland/proxy/admin/leader
func (w *etcdWrapper) leaderNode(service string) string {
	return fmt.Sprintf("%s/leader", apputil.EtcdPathAppPrefix(service))
}

func (w *etcdWrapper) nodeAppContainerHb(service string) string {
	return fmt.Sprintf("%s/containerhb/", apputil.EtcdPathAppPrefix(service))
}

// 存储分配当前关系
func (w *etcdWrapper) nodeAppShard(service string) string {
	return fmt.Sprintf("%s/shard/", apputil.EtcdPathAppPrefix(service))
}

// /borderland/app/proxy/shard/业务自己定义的shard id
func (w *etcdWrapper) nodeAppShardId(service, id string) string {
	return fmt.Sprintf("%s/shard/%s", apputil.EtcdPathAppPrefix(service), id)
}

func (w *etcdWrapper) nodeAppShardHb(service string) string {
	return fmt.Sprintf("%s/shardhb/", apputil.EtcdPathAppPrefix(service))
}

// /borderland/proxy/task
// 如果app的task节点存在任务，不能产生新的新的任务，必须等待ack完成
func (w *etcdWrapper) nodeAppTask(service string) string {
	return fmt.Sprintf("%s/task", apputil.EtcdPathAppPrefix(service))
}

// /borderland/proxy/admin/containerhb/
func (w *etcdWrapper) nodeAppHbContainer(service string) string {
	return fmt.Sprintf("%s/containerhb/", apputil.EtcdPathAppPrefix(service))
}

// /borderland/app/proxy/spec 存储app的基本信息
func (w *etcdWrapper) nodeAppSpec(service string) string {
	return fmt.Sprintf("%s/spec", apputil.EtcdPathAppPrefix(service))
}
