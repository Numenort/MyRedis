package core

import (
	"hash/crc32"
	"myredis/interface/myredis"
	"myredis/protocol"
	"strings"
)

const SlotCount int = 1024

const getCommittedIndexCommand = "raft.committedindex"

// 将命令转发到指定peer节点执行
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

// 获得 key 对应的 SlotID
func (cluster *Cluster) GetSlot(key string) uint32 {
	return cluster.getSlotImpl(key)
}

// 根据 slot 获取目标节点 ID
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

// 在当前节点执行命令
func (cluster *Cluster) LocalExec(conn myredis.Connection, cmdLine [][]byte) myredis.Reply {
	return cluster.db.Exec(conn, cmdLine)
}

func (cluster *Cluster) LocalExecWithLock(conn myredis.Connection, cmdLine [][]byte) myredis.Reply {
	return cluster.db.ExecWithLock(conn, cmdLine)
}

// 检查对应的 keys 是否存在
func (cluster *Cluster) LocalExists(keys []string) []string {
	var exists []string
	for _, key := range keys {
		_, ok := cluster.db.GetEntity(0, key)
		if ok {
			exists = append(exists, key)
		}
	}
	return exists
}
