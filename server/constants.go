package main

import (
	"errors"
	"time"
)

const (
	defaultSleepTimeout = 3 * time.Second

	defaultSessionTimeout = 15

	defaultOpTimeout = 3 * time.Second

	defaultShardLoopInterval = 3 * time.Second
)

var (
	errNotExist = errors.New("not exist")

	errParam = errors.New("param err")

	errEtcdNodeExist     = errors.New("etcd: node exist")
	errEtcdValueExist    = errors.New("etcd: value exist")
	errEtcdValueNotMatch = errors.New("etcd: value not match")
)
