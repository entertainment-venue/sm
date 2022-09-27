package etcdutil

import "path"

var (
	pfx string
)

func init() {
	pfx = "/sm"
}

func SetPfx(v string) {
	if v != "" {
		pfx = v
	}
}

func ServicePath(service string) string {
	return path.Join(pfx, "app", service)
}

func ShardPath(service, containerId, shardId string) string {
	// s的命名方式参考开源项目；pd
	return path.Join(ShardDir(service, containerId), shardId)
}

func ShardDir(service, containerId string) string {
	return path.Join(ServicePath(service), "s", containerId) + "/"
}

func ContainerPath(service, id string) string {
	return path.Join(ServicePath(service), "containerhb", id)
}

func LeasePath(service string) string {
	return path.Join(ServicePath(service), "lease")
}

func LeaseBridgePath(service string) string {
	return path.Join(LeasePath(service), "bridge")
}

func LeaseGuardPath(service string) string {
	return path.Join(LeasePath(service), "guard")
}

func LeaseSessionDir(service string) string {
	return path.Join(LeasePath(service), "session")
}

// LeaseSessionPath 作为guard lease过期的监控点，在得到新的lease的时候创建，server可以通过不续约让这个节点过期，
// 这样shardkeeper感知到guard lease被过期，发起shard drop动作，注意
func LeaseSessionPath(service string, container string) string {
	return path.Join(LeasePath(service), "session", container)
}
