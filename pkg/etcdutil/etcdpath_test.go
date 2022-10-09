package etcdutil

import (
	"testing"
)

func Test_EtcdPath(t *testing.T) {
	if ServicePath("foo") != "/sm/app/foo" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if ShardPath("foo", "127.0.0.1:80", "bar") != "/sm/app/foo/s/127.0.0.1:80/bar" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if ShardDir("foo", "127.0.0.1:80") != "/sm/app/foo/s/127.0.0.1:80/" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if ContainerPath("foo", "bar") != "/sm/app/foo/containerhb/bar" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if LeasePath("foo") != "/sm/app/foo/lease" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if LeaseBridgePath("foo") != "/sm/app/foo/lease/bridge" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if LeaseGuardPath("foo") != "/sm/app/foo/lease/guard" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if LeaseSessionDir("foo") != "/sm/app/foo/lease/session" {
		t.Errorf("path error")
		t.SkipNow()
	}

	if LeaseSessionPath("foo", "127.0.0.1:80") != "/sm/app/foo/lease/session/127.0.0.1:80" {
		t.Errorf("path error")
		t.SkipNow()
	}
}
