package main

// 存储shard基础信息，leader和follower可以利用这些信息做shard move
type shard struct {
	id      string
	service string
}
