package main

import (
	"errors"
	"time"
)

const defaultSleepTimeout = 3 * time.Second

const defaultSessionTimeout = 15

const defaultOpTimeout = 3 * time.Second

const defaultShardLoopInterval = 3 * time.Second

var (
	errNotExist     = errors.New("not exist")
	errAlreadyExist = errors.New("already exist")
)
