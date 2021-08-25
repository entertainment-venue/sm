package main

import "github.com/gin-gonic/gin"

func main() {
	r := gin.Default()

	api := containerApi{}

	// 支持borderland内部shard移动
	containerGroup := r.Group("/borderland/container")
	{
		containerGroup.POST("/drop-shard", api.GinContainerDropShard)

		containerGroup.POST("/add-shard", api.GinContainerAddShard)
	}

	// 支持业务app录入基本信息
	appGroup := r.Group("/borderland/app")
	{
		// 应用基础信息，包括service
		appGroup.POST("/add-spec", api.GinAppAddSpec)

		// 业务场景下的分片拆分后，通过该接口录入borderland
		appGroup.POST("/add-shard", api.GinAppAddShard)

		appGroup.POST("/del-shard", api.GinAppDelShard)
	}

	r.Run()
}
