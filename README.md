# sm(shard manager)

Inspire by

* [scaling-services-with-shard-manager](https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/)
* [Shard Manager: A Generic Shard Management Framework for Geo-distributed Applications](https://research.facebook.com/publications/shard-manager-a-generic-shard-management-framework-for-geo-distributed-applications/)
* [Slicer: Auto-Sharding for Datacenter Applications](https://research.google/pubs/pub46921/)
* [MirrorMaker 2.0](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0)
* [The Chubby lock service for loosely-coupled distributed systems](https://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf)

We build sm to handle the backend service which need partition data into several instance(k8s pod or actual machines,
use container below), the data in each container may be seen as a job. sm developed base
on [etcd](https://github.com/etcd-io/etcd), to make sure that:

* When container changed, some shard moves will be made.
* When shard changed, some shard moves will be made.
* When shard or container load changed, shard moves(TODO).

## Table of Contents

- [Getting Started](#getting-started)
    - [Installing](#installing)
- [Concept explanation](#concept-explanation)
    - [Container](#container)
- [Example](#example)
- [Question](#question)
    - [Question](#question)

## Getting Started

### Installing

Install [etcd](https://github.com/etcd-io/etcd/releases), test with etcd-v3.4.13

Install [go](https://golang.org/dl/) >=go1.17.1, and run:

```
go get https://github.com/entertainment-venue/sm/server

go mod tidy

go run main.go --config-file sample.yml
```

## Concept explanation

### Container

`Container` is the resource for shard to run in, and `Container` establish `Session` with etcd, the `Session` is used
for the management of keys in etcd, management meaning that:

* `Container` goes down, heartbeat which from container to etcd server will be ended.
* Only the unchanged properties of app or shard will stay in etcd permanently, make data to be managed as clean as the
  moonlight.

`Container` also define common protocol which need sharded application who want to integrate with sm:

```
type ShardPrimitives interface {
	Add(id string, spec *ShardSpec) error
	Drop(id string) error
}
```

You can implement the `ShardPrimitives` and inject the implementation into the `Container`
with `WithShardPrimitives`, and also wrap common http api to interact with the sm server. The keep http path:

* /sm/admin/add-shard
* /sm/admin/drop-shard

For more information, you can read
the [source code](https://github.com/entertainment-venue/sm/blob/main/pkg/apputil/container.go) for `Container`.

## Example

You can see the test code as tip to understand how to construct you own sharded application:

https://github.com/entertainment-venue/sm/blob/main/client/client_test.go#L13

If you want to no better how the sm server running, see the code below as an entry point:

https://github.com/entertainment-venue/sm/blob/main/server/main.go

## Question

Please feel free to report any issue or optimization suggestion
in [Issues](https://github.com/entertainment-venue/sm/issues)