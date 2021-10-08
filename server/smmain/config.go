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
	Id        string      `json:"id"`
	Service   string      `json:"service"`
	Addr      string      `json:"addr"`
	Endpoints MultiOption `json:"endpoints"`

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
	flag.StringVar(&cfg.Id, "id", "", "Identify current container, should changed after container restarted.")
	flag.StringVar(&cfg.Service, "service", "", "The sharded application service name, should be used in service discovery")
	flag.StringVar(&cfg.Addr, "addr", "", "Http server listen port like ':8888'")
	flag.Var(&cfg.Endpoints, "endpoints", "The etcd cluster server list")
}

func checkSettings() {
	// 传递配置文件，不需要校验其他配置参数
	if cfg.ConfigFile != "" {
		return
	}

	if cfg.Id == "" {
		fmt.Printf("Err: Id require\n")
		usage()
	}
	if cfg.Service == "" {
		fmt.Printf("Err: Service require\n")
		usage()
	}
	if cfg.Addr == "" {
		fmt.Printf("Err: Addr require\n")
		usage()
	}
	if len(cfg.Endpoints) == 0 {
		fmt.Printf("Err: Endpoints require\n")
		usage()
	}
}
