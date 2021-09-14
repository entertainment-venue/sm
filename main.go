package main

import (
	"context"
	"github.com/entertainment-venue/sm/server"
)

func main() {
	if err := server.Run(
		server.WithContext(context.TODO()),
		server.WithId("127.0.0.1:8888"),
		server.WithService("foo.bar"),
		server.WithAddr(":8888"),
		server.WithEndpoints([]string{"127.0.0.1:2379"})); err != nil {
		panic(err)
	}
}
