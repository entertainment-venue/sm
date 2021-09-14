module github.com/entertainment-venue/sm/server

go 1.14

require (
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/entertainment-venue/sm/pkg v0.0.0-20210914035545-18bf43c978cc
	github.com/gin-gonic/gin v1.7.4
	github.com/pkg/errors v0.9.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.1.0

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.6

// https://blog.csdn.net/qq_43442524/article/details/104997539
replace google.golang.org/grpc => google.golang.org/grpc v1.26.0