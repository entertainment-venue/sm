package logutil

import (
	"fmt"
	"net/url"

	"github.com/pkg/errors"

	"go.uber.org/zap"
	"gopkg.in/natefinch/lumberjack.v2"
)

type logRotationConfig struct {
	*lumberjack.Logger
}

type LogOptions struct {
	// 日志存储路径
	Path string
	// 单个文件大小，单位M
	MaxSize int
	// 最多保存文件个数
	MaxBackups int
	// 最多保留天数
	MaxAge int
	// 是否标准输出
	Stdout bool
}

var defaultLogOptions = LogOptions{
	Path:       "./logs/sm.log",
	MaxSize:    1024,
	MaxBackups: 50,
	MaxAge:     3,
	Stdout:     false,
}

type logOptionsFunc func(*LogOptions)

func WithPath(v string) logOptionsFunc {
	return func(o *LogOptions) {
		o.Path = v
	}
}

func WithMaxSize(v int) logOptionsFunc {
	return func(o *LogOptions) {
		o.MaxSize = v
	}
}

func WithMaxBackups(v int) logOptionsFunc {
	return func(o *LogOptions) {
		o.MaxBackups = v
	}
}

func WithMaxAge(v int) logOptionsFunc {
	return func(o *LogOptions) {
		o.MaxAge = v
	}
}

func WithOutput(v bool) logOptionsFunc {
	return func(o *LogOptions) {
		o.Stdout = v
	}
}

// Sync implements zap.Sink
func (logRotationConfig) Sync() error { return nil }

func NewLogger(opt ...logOptionsFunc) (*zap.Logger, error) {
	opts := defaultLogOptions
	for _, o := range opt {
		o(&opts)
	}

	cfg := logRotationConfig{
		&lumberjack.Logger{
			// 每个文件1g
			MaxSize: opts.MaxSize,

			// 最多50个文件
			MaxBackups: opts.MaxBackups,

			// 最多保留3天
			MaxAge: opts.MaxAge,
		},
	}
	if err := zap.RegisterSink("rotate", func(u *url.URL) (zap.Sink, error) {
		cfg.Filename = u.Path[1:]
		return &cfg, nil
	}); err != nil {
		return nil, errors.Wrap(err, "")
	}

	zap.AddCallerSkip(1)
	zapCfg := zap.NewProductionConfig()

	zapCfg.OutputPaths = []string{fmt.Sprintf("rotate://%s", opts.Path)}
	if opts.Stdout {
		zapCfg.OutputPaths = append(zapCfg.OutputPaths, "stdout")
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return logger, nil
}
