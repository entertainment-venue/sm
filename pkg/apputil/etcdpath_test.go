package apputil

import "testing"

func Test_EtcdPath(t *testing.T) {
	if AppRootPath("foo") != "/sm/app/foo" {
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
}
