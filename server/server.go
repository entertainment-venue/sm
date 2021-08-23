package main

import (
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

type LoadUploader interface {
	// 上报sm各shard的load信息，提供给leader用于做计算
	Upload()
}

type Closer interface {
	Close()
}
