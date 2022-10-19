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

package commonutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func Test_tickerLoop(t *testing.T) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)

	wg.Add(1)
	go TickerLoop(
		ctx,
		LogErrFunc,
		time.Second,
		func(ctx context.Context) error {
			fmt.Println("test fn " + time.Now().String())
			return nil
		},
	)

	go func() {
		for {
			select {
			case <-time.After(3 * time.Second):
				cancel()
				time.Sleep(1 * time.Second)
				wg.Done()
			}
		}
	}()

	wg.Wait()
	fmt.Println("TestTickerLoop exit")
}
