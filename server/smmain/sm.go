package smmain

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/entertainment-venue/sm/pkg/logutil"
	"github.com/entertainment-venue/sm/server/smserver"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type AppMixer interface {
	Addr() string
}

// defaultAppMixer 内置 Resolver
type defaultAppMixer struct {
	port string
}

func (r defaultAppMixer) Addr() string {
	return fmt.Sprintf("%s:%s", smserver.GetLocalIP(), r.port)
}

type serverConfig struct {
	Service    string   `yaml:"service"`
	Port       string   `yaml:"port"`
	Endpoints  []string `yaml:"endpoints"`
	EtcdPrefix string   `yaml:"etcdPrefix"`
}

func (cfg *serverConfig) validate() {
	if cfg.Service == "" {
		panic("Err: Service require")
	}
	if cfg.Port == "" {
		panic("Err: Port require")
	}
	if len(cfg.Endpoints) == 0 {
		panic("Err: Endpoints require")
	}
	if cfg.EtcdPrefix == "" {
		panic("Err: EtcdPrefix require")
	}
}

func StartSM() error {
	// 配置加载
	flag.Parse()
	var cfgPath = flagCfg.ConfigFile
	if cfgPath == "" {
		panic("no config file")
	}
	data, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return errors.Wrap(err, "")
	}

	srvCfg := &serverConfig{}
	if err := yaml.Unmarshal(data, &srvCfg); err != nil {
		return errors.Wrap(err, "")
	}
	srvCfg.validate()

	addr := defaultAppMixer{port: srvCfg.Port}.Addr()

	lg, zapError := logutil.NewLogger(logutil.WithStdout(false))
	if zapError != nil {
		fmt.Printf("error creating zap logger %v", zapError)
		os.Exit(1)
	}
	defer lg.Sync()

	srv, err := smserver.NewServer(
		smserver.WithId(addr),
		smserver.WithService(srvCfg.Service),
		smserver.WithAddr(fmt.Sprintf(":%s", srvCfg.Port)),
		smserver.WithEndpoints(srvCfg.Endpoints),
		smserver.WithEtcdPrefix(srvCfg.EtcdPrefix))
	if err != nil {
		lg.Panic(
			"NewServer error",
			zap.Reflect("flagCfg", flagCfg),
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
	lg.Info("ShardManager exit", zap.String("addr", addr))

	return nil
}
