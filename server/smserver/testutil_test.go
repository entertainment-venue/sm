package smserver

import "testing"

func Test_newTestShardServer(t *testing.T) {
	tests := []struct {
		service     string
		containerId string
		endpoints   []string
		addr        string
	}{
		{
			service:     "foo.bar2",
			containerId: "127.0.0.1:8802",
			endpoints:   []string{"127.0.0.1:2379"},
			addr:        ":8802",
		},
		{
			service:     "foo.bar2",
			containerId: "127.0.0.1:8803",
			endpoints:   []string{"127.0.0.1:2379"},
			addr:        ":8803",
		},
		{
			service:     "foo.bar3",
			containerId: "127.0.0.1:8804",
			endpoints:   []string{"127.0.0.1:2379"},
			addr:        ":8804",
		},
	}

	for _, tt := range tests {
		newTestShardServer(tt.service, tt.containerId, tt.endpoints, tt.addr)
	}

	stopped := make(chan struct{})
	<-stopped
}
