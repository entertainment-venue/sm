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
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

var (
	ttLogger, _ = zap.NewProduction()
)

func Test_newOperator(t *testing.T) {
	ctr, err := launchServerContainer(context.TODO(), ttLogger, "127.0.0.1:8888", "foo.bar")
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	op, err := newOperator(ctr, "foo.bar")
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	go func() {
		time.Sleep(60 * time.Second)
		op.Close()
	}()
}

func Test_remove(t *testing.T) {
	ctr, err := launchServerContainer(context.TODO(), ttLogger, "127.0.0.1:8888", "foo.bar")
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	o := operator{}
	o.parent = ctr

	if err := o.remove(context.TODO(), "1", "foo.bar"); err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}
}

func Test_dropAndAdd(t *testing.T) {
	ctr, err := launchServerContainer(context.TODO(), ttLogger, "127.0.0.1:8888", "foo.bar")
	if err != nil {
		t.Errorf("err: %+v", err)
		t.SkipNow()
	}

	o := operator{}
	o.parent = ctr
	o.hc = newHttpClient()

	// ma := moveAction{
	// 	Service:     "foo.bar",
	// 	ShardId:     "1",
	// 	AddEndpoint: "127.0.0.1:8888",
	// }
	// o.dropAndAdd(&ma)

	ma := moveAction{
		Service:     "foo.bar",
		ShardId:     "1",
		AddEndpoint: "127.0.0.1:8888",
		AllowDrop:   true,
	}
	o.dropAndAdd(context.TODO(), &ma)
}
