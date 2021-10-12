package smmain

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/entertainment-venue/sm/server/smserver"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

func startSM() error {
	flag.Parse()
	checkSettings()

	if cfg.ConfigFile != "" {
		data, err := ioutil.ReadFile(cfg.ConfigFile)
		if err != nil {
			return errors.Wrap(err, "")
		}

		if err := yaml.Unmarshal([]byte(data), &cfg); err != nil {
			return errors.Wrap(err, "")
		}

		cfg.ConfigFile = ""
		checkSettings()
	}

	lg, zapError := NewSMLogger()
	if zapError != nil {
		fmt.Printf("error creating zap logger %v", zapError)
		os.Exit(1)
	}
	defer lg.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error)
	go handleSigs(cancel, lg)

	if _, err := smserver.NewServer(
		smserver.WithId(cfg.Id),
		smserver.WithService(cfg.Service),
		smserver.WithAddr(cfg.Addr),
		smserver.WithEndpoints(cfg.Endpoints),
		smserver.WithCtx(ctx),
		smserver.WithStopped(stopped),
		smserver.WithLogger(lg)); err != nil {
		lg.Panic("failed to start sm server", zap.Reflect("cfg", cfg))
	}

	<-stopped
	lg.Info("sm exit", zap.Reflect("cfg", cfg))

	return nil
}

func handleSigs(cancel context.CancelFunc, lg *zap.Logger) {
	sigChan := make(chan os.Signal)

	signals := []os.Signal{
		syscall.SIGINT,
		syscall.SIGTERM,
	}

	signal.Notify(sigChan, signals...)

	for {
		sig := <-sigChan
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			lg.Warn("Received exit signal", zap.String("sig", sig.String()))
			cancel()
			return
		default:
			lg.Warn("Received unexpected signal", zap.String("sig", sig.String()))
		}
	}
}
