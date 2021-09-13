package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/entertainment-venue/borderland/pkg/apputil"
	"github.com/gin-gonic/gin"
)

type containerApi struct {
	cr *serverContainer
}

type appSpec struct {
	// 目前app的spec更多承担的是管理职能，shard配置的一个起点，先只配置上service，可以唯一标记一个app
	Service string `json:"service"`

	CreateTime int64 `json:"createTime"`
}

func (s *appSpec) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (g *containerApi) GinAppAddSpec(c *gin.Context) {
	var req appSpec
	if err := c.ShouldBind(&req); err != nil {
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	Logger.Printf("req: %v", req)

	//  写入app spec和app task节点在一个tx
	var (
		nodes  []string
		values []string
	)
	nodes = append(nodes, g.cr.ew.nodeAppSpec(req.Service))
	nodes = append(nodes, g.cr.ew.nodeAppTask(req.Service))
	values = append(values, req.String())
	values = append(values, "")
	if err := g.cr.Client.CreateAndGet(context.Background(), nodes, values, clientv3.NoLease); err != nil {
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

type appAddShardRequest struct {
	ShardId string `json:"shardId"`

	// 为哪个业务app增加shard
	Service string `json:"service"`

	// 业务app自己定义task内容
	Task string `json:"task"`
}

func (r *appAddShardRequest) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

func (g *containerApi) GinAppAddShard(c *gin.Context) {
	var req appAddShardRequest
	if err := c.ShouldBind(&req); err != nil {
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	Logger.Printf("req: %v", req)

	spec := apputil.ShardSpec{
		Service:    req.Service,
		Task:       req.Task,
		UpdateTime: time.Now().Unix(),
	}

	// 区分更新和添加
	// 如果是添加，等待负责该app的shard做探测即可
	// 如果是更新，shard是不允许更新的，这种更新的相当于shard工作内容的调整
	if err := g.cr.Client.CreateAndGet(context.Background(), []string{apputil.EtcdPathAppShardId(req.Service, req.ShardId)}, []string{spec.String()}, clientv3.NoLease); err != nil {
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

/*
type appDelShardRequest struct {
	ShardId string `json:"shardId"`
	Service string `json:"service"`
}

func (g *containerApi) GinAppDelShard(c *gin.Context) {
	var req appDelShardRequest
	if err := c.ShouldBind(&req); err != nil {
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	Logger.Printf("req: %v", req)

	resp, err := g.cr.ew.get(context.Background(), g.cr.ew.nodeAppShardId(req.Service, req.ShardId), nil)
	if err != nil {
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if resp.Count == 0 {
		err = errors.Errorf("Failed to get serverShard %s content in service %s", req.ShardId, req.Service)
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var spec shardSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &spec); err != nil {
		Logger.Printf("err: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !spec.Deleted {
		spec.Deleted = true

		if err := g.cr.ew.update(context.Background(), g.cr.ew.nodeAppShardId(req.Service, req.ShardId), spec.String()); err != nil {
			Logger.Printf("err: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{})
}
*/
