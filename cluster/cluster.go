package cluster

import (
	"myredis/cluster/core"
	"myredis/cluster/raft"
	"myredis/config"
	"myredis/lib/logger"
	"os"
	"path"
)

// 创建并启动集群中的一个节点
func MakeCluster() *core.Cluster {
	raftPath := path.Join(config.Properties.Dir, "raft")
	err := os.MkdirAll(raftPath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	cluster, err := core.NewCluster(&core.Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: config.Properties.AnnounceAddress(),
			RaftListenAddr:     config.Properties.RaftListenAddr,
			RaftAdvertiseAddr:  config.Properties.RaftAdvertiseAddr,
			Dir:                raftPath,
		},
		StartAsSeed: config.Properties.ClusterAsSeed,
		JoinAddress: config.Properties.ClusterSeed,
		Master:      config.Properties.MasterInCluster,
	})
	if err != nil {
		logger.Error(err.Error())
		panic(err)
	}
	return cluster
}
