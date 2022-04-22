package client

import (
	"net/url"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/natefinch/lumberjack.v2"
)

type logRotationConfig struct {
	*lumberjack.Logger
}

// Sync implements zap.Sink
func (logRotationConfig) Sync() error { return nil }

func NewLogger() (*zap.Logger, error) {
	cfg := logRotationConfig{
		&lumberjack.Logger{
			// 每个文件1g
			MaxSize: 1024,

			// 50g文件
			MaxBackups: 50,

			// 最多保留3天
			MaxAge: 3,
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
	zapCfg.OutputPaths = []string{"rotate://./log/sm.log", "stdout"}
	logger, err := zapCfg.Build()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return logger, nil
}
