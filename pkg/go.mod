module github.com/entertainment-venue/sm/pkg

go 1.14

require (
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/gin-gonic/gin v1.7.4
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/shirou/gopsutil/v3 v3.21.8
	go.uber.org/zap v1.19.1
	google.golang.org/genproto v0.0.0-20210903162649-d08c68adba83 // indirect
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.6

// https://blog.csdn.net/qq_43442524/article/details/104997539
replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
