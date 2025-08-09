package core

import (
	"myredis/cluster/raft"
	"myredis/datastruct/set"
	"myredis/interface/database"
	"sync"
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

type slotsManage struct {
	mu            *sync.RWMutex
	slots         map[uint32]*slotStatus
	importingTask *raft.MigratingTask
}

type slotStatus struct {
	mu    *sync.RWMutex
	state int
	keys  *set.Set

	exportSnapshot *set.Set
	dirtyKeys      *set.Set
}
