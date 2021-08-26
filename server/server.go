package main

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

// StdLogger is used to log error messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var Logger StdLogger = log.New(os.Stdout, "[LRMF] ", log.LstdFlags|log.Lshortfile)

type LoadUploader interface {
	// 上报sm各shard的load信息，提供给leader用于做计算
	Upload()
}

type Closer interface {
	Close()
}

type borderlandOptions struct {
	id      string
	service string

	etcdEndpoints []string
}

var defaultOpts = borderlandOptions{}

type ResolveServiceFunc func(ctx context.Context) ([]string, error)

type BorderlandOptionsFunc func(options *borderlandOptions)

func WithId(v string) BorderlandOptionsFunc {
	return func(options *borderlandOptions) {
		options.id = v
	}
}

func WithService(v string) BorderlandOptionsFunc {
	return func(options *borderlandOptions) {
		options.service = v
	}
}

func WithEtcdEndpoints(v []string) BorderlandOptionsFunc {
	return func(options *borderlandOptions) {
		options.etcdEndpoints = v
	}
}

func Run(ctx context.Context, fn ...BorderlandOptionsFunc) error {
	opts := defaultOpts
	for _, f := range fn {
		f(&opts)
	}

	cr, err := newContainer(opts.id, opts.service, opts.etcdEndpoints)
	if err != nil {
		return errors.Wrap(err, "")
	}

	api := containerApi{cr}

	r := gin.Default()

	// 支持borderland内部shard移动
	containerGroup := r.Group("/borderland/container")
	{
		containerGroup.POST("/drop-shard", api.GinContainerDropShard)

		containerGroup.POST("/add-shard", api.GinContainerAddShard)
	}

	// 支持业务app录入基本信息
	appGroup := r.Group("/borderland/app")
	{
		// 应用基础信息，包括service
		appGroup.POST("/add-spec", api.GinAppAddSpec)

		// 业务场景下的分片拆分后，通过该接口录入borderland
		appGroup.POST("/add-shard", api.GinAppAddShard)

		appGroup.POST("/del-shard", api.GinAppDelShard)
	}

	if err := r.Run(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}
