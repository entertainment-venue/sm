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

package smserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type moveAction struct {
	Service      string `json:"service"`
	ShardId      string `json:"shardId"`
	DropEndpoint string `json:"dropEndpoint"`
	AddEndpoint  string `json:"addEndpoint"`

	// Spec 存储分片具体信息
	Spec *storage.ShardSpec `json:"spec"`
}

func (action *moveAction) String() string {
	b, _ := json.Marshal(action)
	return string(b)
}

type moveActionList []*moveAction

func (l *moveActionList) String() string {
	b, _ := json.Marshal(l)
	return string(b)
}

// Len 4 unit test
func (l moveActionList) Len() int { return len(l) }
func (l moveActionList) Less(i, j int) bool {
	return l[i].ShardId < l[j].ShardId
}
func (l moveActionList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// operator 负责下发http请求
type operator struct {
	lg *zap.Logger

	// operator 属于接入业务的service
	service string

	httpClient *http.Client
}

func newOperator(lg *zap.Logger, service string) *operator {
	return &operator{
		lg:         lg,
		service:    service,
		httpClient: newHttpClient(),
	}
}

// move 明确参数类型，预防编程错误
func (o *operator) move(mal moveActionList) error {
	o.lg.Info(
		"start move",
		zap.Reflect("mal", mal),
	)

	var (
		// 增加重试机制
		retry   = 1
		counter = 0
		succ    bool
	)
	for counter <= retry {
		if counter > 0 {
			time.Sleep(defaultSleepTimeout)
		}

		g := new(errgroup.Group)
		for _, ma := range mal {
			ma := ma
			g.Go(func() error {
				return o.dropOrAdd(ma)
			})
		}
		if err := g.Wait(); err != nil {
			o.lg.Error(
				"Wait err",
				zap.Error(err),
			)
			counter++
		} else {
			succ = true
			break
		}
	}

	o.lg.Info(
		"complete move",
		zap.Bool("succ", succ),
		zap.Reflect("mal", mal),
	)
	return nil
}

func (o *operator) dropOrAdd(ma *moveAction) error {
	if ma.DropEndpoint != "" {
		if err := o.send(ma.ShardId, ma.Spec, ma.DropEndpoint, "drop"); err != nil {
			return errors.Wrap(err, "")
		}
	}

	if ma.AddEndpoint != "" {
		if err := o.send(ma.ShardId, ma.Spec, ma.AddEndpoint, "add"); err != nil {
			return errors.Wrap(err, "")
		}
	}

	o.lg.Info(
		"dropOrAdd success",
		zap.Reflect("ma", ma),
	)
	return nil
}

func (o *operator) send(id string, spec *storage.ShardSpec, endpoint string, action string) error {
	msg := apputil.ShardMessage{Id: id, Spec: spec}
	b, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "")
	}

	urlStr := fmt.Sprintf("http://%s/sm/admin/%s-shard", endpoint, action)
	req, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewBuffer(b))
	if err != nil {
		o.lg.Error(
			"NewRequest error",
			zap.String("urlStr", urlStr),
			zap.Error(err),
		)
		return errors.Wrap(err, "")
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := o.httpClient.Do(req)
	if err != nil {
		o.lg.Error(
			"Do error",
			zap.String("urlStr", urlStr),
			zap.Error(err),
		)
		return errors.Wrap(err, "")
	}
	defer resp.Body.Close()
	rb, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		o.lg.Error(
			"status not 200",
			zap.String("urlStr", urlStr),
			zap.Int("status", resp.StatusCode),
			zap.ByteString("resp", rb),
		)
		return errors.New("FAILED to send")
	}

	o.lg.Info(
		"send success",
		zap.String("urlStr", urlStr),
		zap.Reflect("msg", msg),
		zap.ByteString("response", rb),
	)
	return nil
}
