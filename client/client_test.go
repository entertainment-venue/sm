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

package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/gin-gonic/gin"
)

func TestNewClient(t *testing.T) {
	port := 8888
	ginSrv := gin.Default()
	_, err := NewClient(ClientWithRouter(ginSrv),
		ClientWithContainerId(fmt.Sprintf("%s:%d", "127.0.0.1", port)),
		ClientWithEtcdAddr([]string{"127.0.0.1:2379"}),
		ClientWithService("proxy.dev"),
		ClientWithImplementation(&testShard{ids: make(map[string]string)}))
	if err != nil {
		log.Fatal(err)
	}
	_ = ginSrv.Run(fmt.Sprintf(":%d", port))
}

type testShard struct {
	lock sync.Mutex
	ids  map[string]string
}

func (s *testShard) Add(ctx context.Context, id string, spec *apputil.ShardSpec) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.ids[id] = ""
	fmt.Printf("add op for %s %+v\n", id, *spec)
	return nil
}

func (s *testShard) Drop(ctx context.Context, id string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.ids, id)
	fmt.Printf("drop op %s\n", id)
	return nil
}

func (s *testShard) Load(ctx context.Context, id string) (string, error) {
	fmt.Printf("load op %s\n", id)
	return "", nil
}

func (s *testShard) Shards(ctx context.Context) ([]string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var r []string
	for id, _ := range s.ids {
		r = append(r, id)
	}
	fmt.Printf("shards op %s\n", r)
	return r, nil
}
