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
	"net"
	"net/http"
	"time"
)

type ArmorMap map[string]string

func (m ArmorMap) KeyList() []string {
	var r []string
	for k := range m {
		r = append(r, k)
	}
	return r
}

func (m ArmorMap) KeyMap() map[string]struct{} {
	r := make(map[string]struct{})
	for k := range m {
		r[k] = struct{}{}
	}
	return r
}

func (m ArmorMap) ValueList() []string {
	var r []string
	for _, v := range m {
		if v == "" {
			continue
		}
		r = append(r, v)
	}
	return r
}

func (m ArmorMap) Exist(k string) bool {
	_, ok := m[k]
	return ok
}

func (m ArmorMap) SwapKV() map[string][]string {
	r := make(map[string][]string)
	for k, v := range m {
		if _, ok := r[v]; !ok {
			r[v] = []string{k}
		} else {
			r[v] = append(r[v], k)
		}
	}
	return r
}

func newHttpClient() *http.Client {
	httpDialContextFunc := (&net.Dialer{Timeout: 1 * time.Second, DualStack: true}).DialContext
	return &http.Client{
		Transport: &http.Transport{
			DialContext: httpDialContextFunc,

			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 0,

			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 50,
		},
		Timeout: 3 * time.Second,
	}
}
