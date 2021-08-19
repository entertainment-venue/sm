package main

// container和shard上报两个维度的load，leader(sm)或者shard(app)探测到异常，会发布任务出来，operator就是这个任务的执行者
type Operator interface {
	// sm的shard需要能为接入app提供shard移动的能力，且保证每个任务被执行掉，所以任务会绑定在shard，防止sm的shard移动导致任务没人干
	Move()

	// TODO k8s相关
	Scale()
}

type operator struct {
}
