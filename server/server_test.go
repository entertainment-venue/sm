package server

import "testing"

func Test_NewServer(t *testing.T) {
	_, err := NewServer(
		WithId("127.0.0.1:8888"),
		WithService("foo.bar"),
		WithAddr(":8888"),
		WithEndpoints([]string{"127.0.0.1:2379"}))
	if err != nil {
		panic(err)
	}

	stopch := make(chan struct{})
	<-stopch
}
