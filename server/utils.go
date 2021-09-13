package server

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
