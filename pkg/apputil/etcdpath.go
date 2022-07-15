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

package apputil

import (
	"path"
)

var (
	// smPrefix 需要可配置
	smPrefix = "/sm"
)

func InitEtcdPrefix(prefix string) {
	if prefix == "" {
		panic("prefix should not be empty")
	}
	smPrefix = prefix
}

func AppRootPath(service string) string {
	return path.Join(smPrefix, "app", service)
}

func ContainerPath(service, id string) string {
	return path.Join(AppRootPath(service), "containerhb", id)
}

func LeasePath(service string) string {
	return path.Join(AppRootPath(service), "lease")
}

func LeaseBridgePath(service string) string {
	return path.Join(LeasePath(service), "bridge")
}

func LeaseGuardPath(service string) string {
	return path.Join(LeasePath(service), "guard")
}
