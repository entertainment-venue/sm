package smmain

import (
	"flag"
	"fmt"
	"os"
)

// MultiOption copy from goreplay setttings.go
type MultiOption []string

func (h *MultiOption) String() string {
	return fmt.Sprint(*h)
}

// Set gets called multiple times for each flag with same name
func (h *MultiOption) Set(value string) error {
	*h = append(*h, value)
	return nil
}

type Config struct {
	Service   string      `json:"service"`
	Port      string      `json:"port"`
	Endpoints MultiOption `json:"endpoints"`

	EtcdPrefix string `json:"etcdPrefix"`

	// TODO 支持配置文件
	ConfigFile string `json:"config-file"`
}

var cfg Config

func usage() {
	fmt.Print("Scaling services with sm(Shard Manager), easy to build sharded application.\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func init() {
	flag.Usage = usage
	flag.StringVar(&cfg.ConfigFile, "config-file", "", "You can use config file to save your common config.")
	flag.StringVar(&cfg.Service, "service", "", "The sharded application service name, should be used in service discovery")
	flag.StringVar(&cfg.Port, "port", "", "Http server listen port like '8888'")
	flag.Var(&cfg.Endpoints, "endpoints", "The etcd cluster server list")
	flag.StringVar(&cfg.EtcdPrefix, "", "/sm", "Etcd namespace, default '/sm'")
}

func checkSettings() {
	// 传递配置文件，不需要校验其他配置参数
	if cfg.ConfigFile != "" {
		return
	}

	if cfg.Service == "" {
		fmt.Printf("Err: Service require\n")
		usage()
	}
	if cfg.Port == "" {
		fmt.Printf("Err: Port require\n")
		usage()
	}
	if len(cfg.Endpoints) == 0 {
		fmt.Printf("Err: Endpoints require\n")
		usage()
	}
	if cfg.EtcdPrefix == "" {
		fmt.Printf("Err: EtcdPrefix require\n")
		usage()
	}
}
