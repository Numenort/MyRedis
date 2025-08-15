package core

import (
	"hash/crc32"
	"myredis/interface/myredis"
	"myredis/protocol"
	"strings"
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

// 默认实现：根据 slotID 选择目标节点
func defaultPickNodeImpl(cluster *Cluster, slotID uint32) string {
	return cluster.raftNode.FSM.PickNode(slotID)
}

// 默认实现：将 key 映射为 SlotID
func defaultGetSlotImpl(cluster *Cluster, key string) uint32 {
	partitionKey := GetPartitionKey(key)
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(SlotCount)
}

// 提取分区标记
func GetPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}
