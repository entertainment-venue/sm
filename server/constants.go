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
