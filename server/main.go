package main

import (
	"github.com/entertainment-venue/sm/server/smmain"
)

// main 默认程序入口，也可以直接使用 smmain.StartSM 传入自定义的 smmain.Resolver
func main() {
	if err := smmain.StartSM(nil); err != nil {
		panic(err)
	}
}
