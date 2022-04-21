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

type AppMixer interface {
	Addr() string
}

// defaultAppMixer 内置 Resolver
type defaultAppMixer struct {
	port string
}

func (r *defaultAppMixer) Addr() string {
	return fmt.Sprintf("%s:%s", smserver.GetLocalIP(), r.port)
}

type serverOptions struct {
	// appMixer 提取应用信息，目前只有k8s场景下的Addr需要应用提供出来
	appMixer AppMixer

	// cfgPath 外部传入yaml格式的配置文件
	cfgPath string
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

type ServerOption func(options *serverOptions)

func WithAppMixer(v AppMixer) ServerOption {
	return func(options *serverOptions) {
		options.appMixer = v
	}
}

func WithCfgPath(v string) ServerOption {
	return func(options *serverOptions) {
		options.cfgPath = v
	}
}

func StartSM(opts ...ServerOption) error {
	ops := &serverOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(ops)
		}
	}

	// 配置加载
	var cfgPath string
	if ops.cfgPath == "" {
		flag.Parse()
		cfgPath = flagCfg.ConfigFile
	} else {
		cfgPath = ops.cfgPath
	}
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

	if ops.appMixer == nil {
		ops.appMixer = &defaultAppMixer{port: srvCfg.Port}
	}

	lg, zapError := NewSMLogger()
	if zapError != nil {
		fmt.Printf("error creating zap logger %v", zapError)
		os.Exit(1)
	}
	defer lg.Sync()

	srv, err := smserver.NewServer(
		smserver.WithLogger(lg),
		smserver.WithId(ops.appMixer.Addr()),
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
	lg.Info("ShardManager exit", zap.String("addr", ops.appMixer.Addr()))

	return nil
}
