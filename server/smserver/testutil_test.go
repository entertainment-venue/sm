package smserver

import "testing"

func Test_newTestShardServer(t *testing.T) {
	newTestShardServer()

	stopped := make(chan struct{})
	<-stopped
}
