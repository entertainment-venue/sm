package apputil

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

// 接入方app和sm本身的实现都需要这个方法，实现形式有两种：
// 1 纯工具方法，参数可能复杂些，包含需要的etcd客户端
// 2 接口，调用后做所有事情，定时上传 + 错误回调 + 退出识别
type sysLoadReporter struct {
	container *Container

	node string
}

func ReportSysLoad(ctx context.Context, container *Container, node string) {
	reporter := sysLoadReporter{container: container, node: node}
	stopper := GoroutineStopper{}
	stopper.Run(ctx, reporter.UploadSysLoad)
}

type SysLoad struct {
	VirtualMemoryStat  *mem.VirtualMemoryStat `json:"virtualMemoryStat"`
	CPUUsedPercent     float64                `json:"cpuUsedPercent"`
	DiskIOCountersStat []*disk.IOCountersStat `json:"diskIOCountersStat"`
	NetIOCountersStat  *net.IOCountersStat    `json:"netIOCountersStat"`
}

func (l *SysLoad) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

func (u *sysLoadReporter) UploadSysLoad(ctx context.Context) error {
	ld := SysLoad{}

	// 内存使用比率
	vm, err := mem.VirtualMemory()
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.VirtualMemoryStat = vm

	// cpu使用比率
	cp, err := cpu.Percent(0, false)
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.CPUUsedPercent = cp[0]

	// 磁盘io使用比率
	diskIOCounters, err := disk.IOCounters()
	if err != nil {
		return errors.Wrap(err, "")
	}
	for _, v := range diskIOCounters {
		ld.DiskIOCountersStat = append(ld.DiskIOCountersStat, &v)
	}

	// 网路io使用比率
	netIOCounters, err := net.IOCounters(false)
	if err != nil {
		return errors.Wrap(err, "")
	}
	ld.NetIOCountersStat = &netIOCounters[0]

	if _, err := u.container.Client.Put(ctx, u.node, ld.String(), clientv3.WithLease(u.container.Session.Lease())); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}
