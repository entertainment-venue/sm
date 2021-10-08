package smmain

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

func NewSMLogger() (*zap.Logger, error) {
	cfg := logRotationConfig{
		&lumberjack.Logger{
			MaxSize:    1024 * 1024 * 1024,
			MaxBackups: 7,
			MaxAge:     1,
		},
	}
	if err := zap.RegisterSink("rotate", func(u *url.URL) (zap.Sink, error) {
		cfg.Filename = u.Path[1:]
		return &cfg, nil
	}); err != nil {
		return nil, errors.Wrap(err, "")
	}

	zapCfg := zap.NewProductionConfig()
	zapCfg.OutputPaths = []string{"rotate://./logs/sm.log"}
	logger, err := zapCfg.Build()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return logger, nil
}
