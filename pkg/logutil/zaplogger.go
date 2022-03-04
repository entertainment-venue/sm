package logutil

import (
	"go.uber.org/zap"
)

var _ Logger = new(zaplg)

type zaplg struct {
	lg *zap.SugaredLogger
}

func NewZapLogger(l *zap.Logger) Logger {
	return &zaplg{lg: l.Sugar()}
}

func (z *zaplg) Debug(v ...interface{}) {
	z.lg.Debug(v)
}

func (z *zaplg) Debugf(format string, v ...interface{}) {
	z.lg.Debugf(format, v)
}

func (z *zaplg) Info(v ...interface{}) {
	z.lg.Info(v)
}

func (z *zaplg) Infof(format string, v ...interface{}) {
	z.lg.Infof(format, v)
}

func (z *zaplg) Warn(v ...interface{}) {
	z.lg.Warn(v)
}

func (z *zaplg) Warnf(format string, v ...interface{}) {
	z.lg.Warnf(format, v)
}

func (z *zaplg) Error(v ...interface{}) {
	z.lg.Error(v)
}

func (z *zaplg) Errorf(format string, v ...interface{}) {
	z.lg.Errorf(format, v)
}
