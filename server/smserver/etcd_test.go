package smserver

import "testing"

func Test_Etcd(t *testing.T) {
	nm := nodeManager{smService: "foo"}

	// sm部分

	if nm.SMRootPath() != "/sm/app/foo" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.LeaderPath() != "/sm/app/foo/leader" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.ServicePath("bar") != "/sm/app/foo/service/bar" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.ServiceSpecPath("bar") != "/sm/app/foo/service/bar/spec" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.ShardPath("bar", "s1") != "/sm/app/foo/service/bar/shard/s1" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.WorkerGroupPath("bar") != "/sm/app/foo/service/bar/workerpool" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.WorkerPath("bar", "wg1", "w1") != "/sm/app/foo/service/bar/workerpool/wg1/w1" {
		t.Error("path error")
		t.SkipNow()
	}

	// service部分
	if nm.ExternalServiceDir("bar") != "/sm/app/bar/" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.ExternalShardHbDir("bar") != "/sm/app/bar/shardhb/" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.ExternalContainerHbDir("bar") != "/sm/app/bar/containerhb/" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.ExternalLeaseGuardPath("bar") != "/sm/app/bar/lease/guard" {
		t.Error("path error")
		t.SkipNow()
	}

	if nm.ExternalLeaseBridgePath("bar") != "/sm/app/bar/lease/bridge" {
		t.Error("path error")
		t.SkipNow()
	}
}
