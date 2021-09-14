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

package server

import (
	"errors"
	"time"
)

const (
	defaultSleepTimeout = 3 * time.Second

	// 控制和session的lease绑定的数据的被删除时间
	defaultSessionTimeout = 5

	defaultOpTimeout = 3 * time.Second

	defaultShardLoopInterval = 3 * time.Second
)

var (
	errNotExist = errors.New("not exist")
	errParam    = errors.New("param err")
)
