package main

import (
	"github.com/entertainment-venue/sm/server"
)

func main() {
	_, err := server.NewServer(
		server.WithId("127.0.0.1:8888"),
		server.WithService("foo.bar"),
		server.WithAddr(":8888"),
		server.WithEndpoints([]string{"127.0.0.1:2379"}))
	if err != nil {
		panic(err)
	}

	stopch := make(chan struct{})
	<-stopch
}
