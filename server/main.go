package main

import "github.com/gin-gonic/gin"

func main() {
	r := gin.Default()

	// 支持borderland内部shard移动
	shardGroup := r.Group("/borderland/shard")
	{
		shardGroup.POST("/drop", GinShardDrop)
		shardGroup.POST("/add", GinShardAdd)
	}

	// 支持业务app录入基本信息
	appGroup := r.Group("/borderland/app")
	{
		// 应用基础信息，包括service
		appGroup.POST("/add-spec", GinAppAddSpec)

		// 业务场景下的分片拆分后，通过该接口录入borderland
		appGroup.POST("/add-shard", GinAppAddShard)
	}

	r.Run()
}
