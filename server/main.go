package main

import (
	"github.com/entertainment-venue/sm/server/smmain"
)

// main 默认程序入口，也可以直接使用 smmain.Main 传入自定义的 smmain.Resolver
func main() {
	smmain.Main(nil)
}
