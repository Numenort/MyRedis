package core

import (
	"myredis/interface/myredis"
	"myredis/protocol"
)

const SlotCount int = 1024

const getCommittedIndexCommand = "raft.committedindex"

func (cluster *Cluster) Relay(peerID string, c myredis.Connection, cmdLine [][]byte) myredis.Reply {
	if peerID == cluster.SelfID() {
		return cluster.Exec(c, cmdLine)
	}
	client, err := cluster.connections.BorrowPeerClient(peerID)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.connections.ReturnPeerClient(client)
	}()
	return client.Send(cmdLine)
}

func (cluster *Cluster) GetSlot(key string) uint32 {
	return cluster.getSlotImpl(key)
}

func (cluster *Cluster) PickNode(slotID uint32) string {
	return cluster.pickNodeImpl(slotID)
}
