package core

import (
	"myredis/cluster/raft"
	"myredis/interface/database"
)

type Cluster struct {
	raftNode    raft.Node
	db          database.DBEngine
	connections ConnectionFactory
	config      *Config
}

type Config struct {
	raft.RaftConfig
}
