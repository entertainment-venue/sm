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
	ConfigFile string `json:"config-file"`
}

var flagCfg Config

func usage() {
	fmt.Print("Scaling services with sm(Shard Manager), easy to build sharded application.\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func init() {
	flag.Usage = usage
	flag.StringVar(&flagCfg.ConfigFile, "config-file", "", "You can use config file to save your common config.")
}
