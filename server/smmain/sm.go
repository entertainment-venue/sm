package smmain

import (
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

		if err := yaml.Unmarshal(data, &cfg); err != nil {
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

	srv, err := smserver.NewServer(
		smserver.WithId(fmt.Sprintf("%s:%s", smserver.GetLocalIP(), cfg.Port)),
		smserver.WithService(cfg.Service),
		smserver.WithAddr(fmt.Sprintf(":%s", cfg.Port)),
		smserver.WithEndpoints(cfg.Endpoints),
		smserver.WithLogger(lg),
		smserver.WithEtcdPrefix(cfg.EtcdPrefix))
	if err != nil {
		lg.Panic(
			"NewServer error",
			zap.Reflect("cfg", cfg),
			zap.Error(err),
		)
	}

	// 监听信号
	go func() {
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
				srv.Close()
				return
			default:
				lg.Warn("Received unexpected signal", zap.String("sig", sig.String()))
			}
		}
	}()

	<-srv.Done()
	lg.Info("ShardManager exit", zap.Reflect("cfg", cfg))

	return nil
}
