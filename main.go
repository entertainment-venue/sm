package main

import (
	"context"

	"github.com/entertainment-venue/borderland/server"
)

func main() {
	if err := server.Run(
		context.Background(),
		server.WithId("127.0.0.1:8888"),
		server.WithAddr(":8888"),
		server.WithService("foo.bar"),
		server.WithEtcdEndpoints([]string{"127.0.0.1:2379"}),
	); err != nil {
		panic(err)
	}
}
