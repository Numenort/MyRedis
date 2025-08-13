package core

import (
	"myredis/cluster/raft"
	"myredis/datastruct/set"
	"myredis/interface/database"
	"sync"
)

// Cluster 代表当前节点在分布式集群中的本地视图和状态管理
type Cluster struct {
	raftNode    *raft.Node
	db          database.DBEngine
	connections ConnectionFactory
	config      *Config

	slotsManager    *slotsManage
	rebalanceManger *rebalanceManager
	transcations    *TranscationManager
	replicaManager  *replicaManager

	closeChan chan struct{}

	getSlotImpl  func(key string) uint32
	pickNodeImpl func(slotID uint32) string
	id_          string
}

type Config struct {
	raft.RaftConfig
}

type slotsManage struct {
	mu            *sync.RWMutex
	slots         map[uint32]*slotStatus
	importingTask *raft.MigratingTask
}

func (ssm *slotsManage) getSlot(index uint32) *slotStatus {
	ssm.mu.RLock()
	slot := ssm.slots[index]
	ssm.mu.RUnlock()
	// 尝试获取 slot
	if slot != nil {
		return slot
	}
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	// 双重检查，防止被修改
	slot = ssm.slots[index]
	if slot != nil {
		return slot
	}
	slot = &slotStatus{
		state: slotStateHosting,
		keys:  set.Make(),
		mu:    &sync.RWMutex{},
	}
	ssm.slots[index] = slot
	return slot
}

const (
	slotStateHosting = iota
	slotStateImporting
	slotStateExporting
)

type slotStatus struct {
	mu    *sync.RWMutex
	state int
	keys  *set.Set // 当前 Slot 管理的所有 key 的集合

	exportSnapshot *set.Set // 导出开始时的 keys 快照
	dirtyKeys      *set.Set // 导出过程中被插入或删除的 key 集合
}

func (cluster *Cluster) SelfID() string {
	if cluster.raftNode == nil {
		return cluster.id_
	}
	return cluster.raftNode.Cfg.ID()
}
