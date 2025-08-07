package raft

import "github.com/hashicorp/raft"

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
