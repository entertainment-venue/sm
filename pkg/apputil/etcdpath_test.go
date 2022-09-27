package apputil

import (
	"testing"

	"github.com/entertainment-venue/sm/pkg/etcdutil"
)

func Test_EtcdPath(t *testing.T) {
	if etcdutil.ServicePath("foo") != "/sm/app/foo" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if etcdutil.ContainerPath("foo", "bar") != "/sm/app/foo/containerhb/bar" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if etcdutil.LeasePath("foo") != "/sm/app/foo/lease" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if etcdutil.LeaseBridgePath("foo") != "/sm/app/foo/lease/bridge" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if etcdutil.LeaseGuardPath("foo") != "/sm/app/foo/lease/guard" {
		t.Errorf("path error")
		t.SkipNow()
	}
}
