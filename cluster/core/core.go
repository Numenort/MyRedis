package core

import (
	"myredis/cluster/raft"
	"myredis/interface/database"
)

type Cluster struct {
	raftNode raft.Node
	db       database.DBEngine
}
