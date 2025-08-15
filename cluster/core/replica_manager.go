// 实现 Redis 集群中副本（replica）的心跳检测与故障转移（failover）机制
// 用于检测主节点（master）是否失联，并在必要时自动触发故障转移。
// 通过接收从节点的心跳、定期检查超时、提议 Raft 日志等方式，实现高可用的主备切换。
// 内部命令：
// - cluster.heartbeat <nodeId> : 从节点向主节点发送心跳
// 函数说明：
// - init()                              : 注册 heartbeat 命令处理函数
// - execHeartbeat(cluster, c, cmdLine)  : 接收从节点心跳，更新其最后活跃时间
// - (*Cluster) sendHearbeat()           : 作为从节点，向主节点发送自身心跳
// - (*Cluster) doFailoverCheck()        : 检查是否有主节点失联（心跳超时），触发故障转移流程
// - (*Cluster) triggerFailover(failed)  : 对失联主节点执行故障转移：提升首个从节点为主，并更新路由
// - (*Cluster) registerOnFailover()     : 注册 Raft 回调，当本节点被切换为从节点时，自动执行 SLAVEOF 指令

package core

import (
	"myredis/cluster/raft"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/protocol"
	"net"
	"sync"
	"time"
)

const heartbeatCommand = "cluster.heartbeat"

const (
	statusNormal = iota
	statusFailing
)

type replicaManager struct {
	mu               sync.RWMutex
	masterHeartbeats map[string]time.Time // 记录每个 Master 最后心跳时间
}

func newReplicaManager() *replicaManager {
	return &replicaManager{
		masterHeartbeats: make(map[string]time.Time),
	}
}

// 接收并处理心跳请求，通知集群仍然存活，在 raft Leader 节点执行
// 命令格式：
//
//	cluster.heartbeat <nodeID>
func execHeartbeat(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(heartbeatCommand)
	}
	nodeID := string(cmdLine[1])
	cluster.replicaManager.mu.Lock()
	cluster.replicaManager.masterHeartbeats[nodeID] = time.Now()
	cluster.replicaManager.mu.Unlock()

	return protocol.MakeOkReply()
}

// 由每个 Redis Master 节点主动调用，向 raft leader 报告存活状况
func (cluster *Cluster) sendHearbeat() {
	leaderConn, err := cluster.BorrowLeaderClient()
	if err != nil {
		logger.Error(err)
	}
	defer cluster.connections.ReturnPeerClient(leaderConn)
	// 向主节点发送心跳消息，证明当前节点还存活
	reply := leaderConn.Send(utils.ToCmdLine(heartbeatCommand, cluster.SelfID()))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		logger.Error(err)
	}
}

const followerTimeout = 10 * time.Second

// 定期检查是否有 Redis Master 节点失联，并触发故障转移。函数只能由 Raft Leader执行
func (cluster *Cluster) doFailoverCheck() {
	// 记录超时节点（即未收到心跳消息）
	var timeoutMasters []*raft.MasterSlave
	// 超时时间节点
	ddl := time.Now().Add(-followerTimeout)
	cluster.replicaManager.mu.RLock()
	// 遍历每个心跳消息
	for masterID, lastTime := range cluster.replicaManager.masterHeartbeats {
		if lastTime.IsZero() {
			// 防止新加入节点被误判超时
			cluster.replicaManager.masterHeartbeats[masterID] = time.Now()
		}
		if lastTime.Before(ddl) {
			slaves := cluster.raftNode.GetSlaves(masterID)
			if slaves != nil && len(slaves.Slaves) > 0 {
				timeoutMasters = append(timeoutMasters, slaves)
			}
		}
	}
	cluster.replicaManager.mu.RUnlock()
	// 对每个超时节点切换主节点
	for _, failed := range timeoutMasters {
		cluster.triggerFailover(failed)
	}
}

// 对指定的已失效 Master 执行故障转移，提升其第一个 Slave 为新 Master
func (cluster *Cluster) triggerFailover(failed *raft.MasterSlave) error {
	// 选择第一个从节点为新的主节点
	newMaster := failed.Slaves[0]
	id := utils.RandString(20)
	// 提交主节点切换任务，通知集群开始 failover
	_, err := cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventStartFailover,
		FailoverTask: &raft.FailoverTask{
			ID:          id,
			OldMasterID: failed.MasterID,
			NewMasterID: newMaster,
		},
	})
	if err != nil {
		return err
	}
	logger.Infof("proposed start failover id=%s, oldMaster=%s, newMaster=%s", id, failed.MasterID, newMaster)

	// 远程节点执行 SLAVEOF NO ONE
	conn, err := cluster.connections.BorrowPeerClient(newMaster)
	if err != nil {
		return err
	}
	defer cluster.connections.ReturnPeerClient(conn)

	reply := conn.Send(utils.ToCmdLine("slaveof", "no", "one"))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		return err
	}

	// 提交故障转移完成日志，通知集群更新路由信息
	_, err = cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventFinishFailover,
		FailoverTask: &raft.FailoverTask{
			ID:          id,
			OldMasterID: failed.MasterID,
			NewMasterID: newMaster,
		},
	})
	if err != nil {
		return err
	}
	logger.Infof("proposed finish failover id=%s, oldMaster=%s, newMaster=%s", id, failed.MasterID, newMaster)
	return nil
}

// 注册一个回调函数，当 Raft 状态机应用到故障转移事件时被调用
// 通知本节点，Redis 集群拓扑已变更，需更新自身的数据复制关系
func (cluster *Cluster) registerOnFailover() {
	cluster.raftNode.SetOnFailover(func(newMaster string) {
		if newMaster == "" || newMaster == cluster.SelfID() {
			return
		}
		// 节点 ID 格式 ip:port
		ip, port, err := net.SplitHostPort(newMaster)
		if err != nil {
			logger.Errorf("illegal new master address: %s", newMaster)
			return
		}
		// 发送 slaveof 命令
		conn := connection.NewSimpleConn()
		ret := cluster.db.Exec(conn, utils.ToCmdLine("slaveof", ip, port))
		if err := protocol.Try2ErrorReply(ret); err != nil {
			logger.Infof("failed to execute SLAVEOF %s:%s: %v", ip, port, err)
			return
		}
	})
}

func init() {
	RegisterCmd(heartbeatCommand, execHeartbeat)
}
