module github.com/entertainment-venue/sm

go 1.14

require (
	github.com/entertainment-venue/sm/pkg v0.0.0-20210922102859-36578cb12d43 // indirect
	github.com/entertainment-venue/sm/server v0.0.0-20210922110036-55bd3647c142 // indirect
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/net v0.0.0-20210917221730-978cfadd31cf // indirect
	golang.org/x/sys v0.0.0-20210921065528-437939a70204 // indirect
	google.golang.org/genproto v0.0.0-20210921142501-181ce0d877f6 // indirect
)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.1.0

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.6

// https://blog.csdn.net/qq_43442524/article/details/104997539
replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
