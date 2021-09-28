package main

import "github.com/entertainment-venue/sm/server/smserver"

func main() {
	_, err := smserver.NewServer(
		smserver.WithId("127.0.0.1:8888"),
		smserver.WithService("foo.bar"),
		smserver.WithAddr(":8888"),
		smserver.WithEndpoints([]string{"127.0.0.1:2379"}))
	if err != nil {
		panic(err)
	}

	stopch := make(chan struct{})
	<-stopch
}
