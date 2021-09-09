package server

import (
	"fmt"
	"github.com/entertainment-venue/borderland/pkg/etcdutil"
	"github.com/pkg/errors"
)

type etcdWrapper struct {
	*etcdutil.EtcdClient

	ctr *container
}

func newEtcdWrapper(endpoints []string) (*etcdWrapper, error) {
	client, err := etcdutil.NewEtcdClient(endpoints)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return &etcdWrapper{EtcdClient: client}, nil
}

func (w *etcdWrapper) nodeAppPrefix(service string) string {
	return fmt.Sprintf("/bd/app/%s", service)
}

// /borderland/proxy/admin/leader
func (w *etcdWrapper) leaderNode(service string) string {
	return fmt.Sprintf("%s/leader", w.nodeAppPrefix(service))
}

func (w *etcdWrapper) nodeAppContainerHb(service string) string {
	return fmt.Sprintf("%s/containerhb/", w.nodeAppPrefix(service))
}

func (w *etcdWrapper) nodeAppContainerIdHb(service, id string) string {
	return fmt.Sprintf("%s/containerhb/%s", w.nodeAppPrefix(service), id)
}

// 存储分配当前关系
func (w *etcdWrapper) nodeAppShard(service string) string {
	return fmt.Sprintf("%s/shard/", w.nodeAppPrefix(service))
}

// /borderland/app/proxy/shard/业务自己定义的shard id
func (w *etcdWrapper) nodeAppShardId(service, id string) string {
	return fmt.Sprintf("%s/shard/%s", w.nodeAppPrefix(service), id)
}

func (w *etcdWrapper) nodeAppShardHb(service string) string {
	return fmt.Sprintf("%s/shardhb/", w.nodeAppPrefix(service))
}

func (w *etcdWrapper) nodeAppShardHbId(service, id string) string {
	return fmt.Sprintf("%s/shardhb/%s", w.nodeAppPrefix(service), id)
}

// /borderland/proxy/task
// 如果app的task节点存在任务，不能产生新的新的任务，必须等待ack完成
func (w *etcdWrapper) nodeAppTask(service string) string {
	return fmt.Sprintf("%s/task", w.nodeAppPrefix(service))
}

// /borderland/proxy/admin/containerhb/
func (w *etcdWrapper) nodeAppHbContainer(service string) string {
	return fmt.Sprintf("%s/containerhb/", w.nodeAppPrefix(service))
}

// /borderland/app/proxy/spec 存储app的基本信息
func (w *etcdWrapper) nodeAppSpec(service string) string {
	return fmt.Sprintf("%s/spec", w.nodeAppPrefix(service))
}
