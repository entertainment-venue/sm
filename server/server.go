package main

import (
	"context"
	"log"
	"os"
)

// StdLogger is used to log error messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var Logger StdLogger = log.New(os.Stdout, "[LRMF] ", log.LstdFlags|log.Lshortfile)

// container需要实现该接口，作为管理指令的接收点
type ShardMover interface {
	Add(ctx context.Context, id string) error
	Drop(ctx context.Context, id string) error
}

// shard需要实现该接口，帮助理解程序设计，不会有app实现多种doer
type ShardDoer interface {
	Stop()
	Heartbeat()
}
