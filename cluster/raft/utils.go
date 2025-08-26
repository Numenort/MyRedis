package raft

import (
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"
)

// 返回 raft 节点自身 ID
func (node *Node) Self() string {
	return node.Cfg.ID()
}

func (node *Node) State() raft.RaftState {
	return node.inner.State()
}

func (node *Node) GetSlaves(id string) *MasterSlave {
	node.FSM.mu.RLock()
	defer node.FSM.mu.RUnlock()
	return node.FSM.MasterSlaves[id]
}

func (node *Node) GetLeaderRedisAddress() string {
	_, id := node.inner.LeaderWithID()
	return string(id)
}

func (node *Node) GetNodes() ([]raft.Server, error) {
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to get raft configuration: %v", err)
	}
	return configFuture.Configuration().Servers, nil
}

// 获取当前 Raft 集群中已被提交的日志索引
func (node *Node) CommittedIndex() (uint64, error) {
	status := node.inner.Stats()
	committedIndex := status["commit_index"]
	return strconv.ParseUint(committedIndex, 10, 64)
}
