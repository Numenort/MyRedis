/* 用于实现集群模式下的动态管理操作
 - cluster.join: 由新节点请求加入，由 Leader 提案加入 Raft 集群并分配角色
 - cluster.migration.changeroute: 迁移完成后，由 Leader 提案更新槽位路由

[外部触发或 doRebalance()]
        ↓
Leader 调用 triggerMigrationTask(task)
        ↓
Propose(EventStartMigrate) → Raft Log → 所有节点 FSM 记录“迁移中”
        ↓
向 target 发送 startmigration 命令 → 开始复制数据
        ↓
数据同步完成
        ↓
调用 cluster.migration.changeroute <taskId>
        ↓
Propose(EventFinishMigrate) → Raft Log
        ↓
waitCommitted(srcNode, logIndex)  → 确认源节点已提交
        ↓
waitCommitted(targetNode, logIndex) → 确认目标节点已提交
        ↓
路由更新完成：slot → targetNode

*/

package core

import (
	"errors"
	"fmt"
	"math"
	"myredis/cluster/raft"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/protocol"
	"strconv"
	"sync"
	"time"
)

const (
	joinClusterCommand          = "cluster.join"                  // 新节点加入集群的命令
	migrationChangeRouteCommand = "cluster.migration.changeroute" // 槽位迁移完成后，用于更新集群路由的内部命令
)

// 处理节点加入集群的请求：只有 Raft Leader 可以处理该请求，其他节点会将请求转发给 Leader。
//
// 命令格式：
//
//	cluster.join <redisAddr> <raftAddr> [masterId]
//
// 参数说明：
//   - redisAddr: 节点对外服务地址（客户端连接使用）
//   - raftAddr: 节点 Raft 通信地址（集群内部通信）
//   - masterId: 可选，指定该节点作为从节点时的主节点 ID
func execJoin(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply(joinClusterCommand)
	}
	// 检查当前节点是否为 Leader
	state := cluster.raftNode.State()
	if state != raft.Leader {
		// 将请求转发给 Leader
		leaderConn, err := cluster.BorrowLeaderClient()
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		defer cluster.connections.ReturnPeerClient(leaderConn)
		// Leader 返回命令结果
		return leaderConn.Send(cmdLine)
	}

	// leader 处理对应逻辑
	redisAddr := string(cmdLine[1])
	raftAddr := string(cmdLine[2])
	// 将对应成员加入 raft 成员中
	err := cluster.raftNode.AddToRaft(redisAddr, raftAddr)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	master := ""
	if len(cmdLine) == 4 {
		master = string(cmdLine[3])
	}
	// 提案，申请加入新节点
	_, err = cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventJoin,
		JoinTask: &raft.JoinTask{
			NodeID: redisAddr,
			Master: master,
		},
	})
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	logger.Infof("Node %s joined cluster via %s (master=%s)", redisAddr, raftAddr, master)
	return protocol.MakeOkReply()
}

// 用于防止并发执行多个负载均衡任务
type rebalanceManager struct {
	mu *sync.Mutex
}

func newRebalanceManager() *rebalanceManager {
	return &rebalanceManager{
		mu: &sync.Mutex{},
	}
}

// 执行一次集群负载均衡，仅由 Leader 调用
func (cluster *Cluster) doRebalance() {
	cluster.rebalanceManger.mu.Lock()
	defer cluster.rebalanceManger.mu.Unlock()

	pendingTasks, err := cluster.makeRebalancePlan()
	if err != nil {
		logger.Errorf("makeRebalancePlan err: %v", err)
		return
	}

	logger.Infof("rebalance plan generated, contains %d tasks", len(pendingTasks))
	if len(pendingTasks) == 0 {
		return // 无需迁移
	}

	for _, task := range pendingTasks {
		// 进行迁移任务
		err := cluster.triggerMigrationTask(task)
		if err != nil {
			logger.Errorf("triggerMigrationTask err: %v", err)
		} else {
			logger.Infof("triggerMigrationTask %s success", task.ID)
		}
	}

}

// 生成负载均衡迁移计划
//  1. 计算每个主节点平均应持有的槽位数（向上取整）
//  2. 找出槽位过多的节点（exportingNodes）和过少的节点（importingNodes）
//  3. 逐个配对，生成 MigratingTask
func (cluster *Cluster) makeRebalancePlan() ([]*raft.MigratingTask, error) {
	var migratings []*raft.MigratingTask

	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		avgSlot := int(math.Ceil(float64(SlotCount) / float64(len(fsm.MasterSlaves))))

		var exportingNodes []string
		var importingNodes []string

		for _, masterSlave := range fsm.MasterSlaves {
			nodeID := masterSlave.MasterID
			nodeSlots := fsm.Node2Slot[nodeID]
			if len(nodeSlots) > avgSlot+1 {
				exportingNodes = append(exportingNodes, nodeID)
			} else if len(nodeSlots) < avgSlot-1 {
				importingNodes = append(importingNodes, nodeID)
			}
		}

		importIndex := 0         // 槽位过少的主节点
		exportIndex := 0         // 槽位过多的主节点
		var exportSlots []uint32 // 当前导出节点剩余的槽位

		for importIndex < len(importingNodes) && exportIndex < len(exportingNodes) {
			// 获取被导出的主节点
			exportNode := exportingNodes[exportIndex]
			// 获取空闲的槽位
			if len(exportSlots) == 0 {
				exportNodeSlots := fsm.Node2Slot[exportNode]
				exportCount := len(exportNodeSlots) - avgSlot
				exportSlots = exportNodeSlots[0:exportCount]
			}

			importNode := importingNodes[importIndex]
			currentImportCount := len(fsm.Node2Slot[importNode])
			requirements := avgSlot - currentImportCount
			// 创建迁移任务
			task := &raft.MigratingTask{
				ID:         utils.RandString(20),
				SrcNode:    exportNode,
				TargetNode: importNode,
			}
			if requirements <= len(exportSlots) {
				// 当前空余槽位足够导入需求
				task.Slots = exportSlots[0:requirements]
				exportSlots = exportSlots[requirements:]
				importIndex++
			} else {
				// 当前导出槽位不足
				task.Slots = exportSlots
				exportSlots = nil
				exportIndex++
			}

			migratings = append(migratings, task)
		}
	})
	return migratings, nil
}

// 启动一个槽位迁移任务
// 步骤：
//  1. 向 Raft 提案 EventStartMigrate 事件
//  2. 向目标节点发送 `startmigration` 命令，开始接收槽位数据
func (cluster *Cluster) triggerMigrationTask(task *raft.MigratingTask) error {
	_, err := cluster.raftNode.Propose(&raft.LogEntry{
		Event:         raft.EventStartMigrate,
		MigratingTask: task,
	})
	if err != nil {
		return fmt.Errorf("propose EventStartMigrate %s failed: %v", task.ID, err)
	}
	logger.Infof("propose EventStartMigrate %s success", task.ID)

	// 发送槽位迁移命令
	cmdLine := utils.ToCmdLine(startMigrationCommand, task.ID, task.SrcNode)
	for _, slotID := range task.Slots {
		slotIdStr := strconv.Itoa(int(slotID))
		cmdLine = append(cmdLine, []byte(slotIdStr))
	}
	// 获取目标节点的连接
	targetNodeConn, err := cluster.connections.BorrowPeerClient(task.TargetNode)
	if err != nil {
		return err
	}
	defer cluster.connections.ReturnPeerClient(targetNodeConn)
	// 目标节点执行命令
	reply := targetNodeConn.Send(cmdLine)
	if protocol.IsOKReply(reply) {
		return nil
	}
	return protocol.MakeErrReply("target node rejected migration")
}

// 等待指定节点将某条 Raft 日志提交到本地 FSM，
// 用于确保源节点和目标节点都已应用了 EventFinishMigrate 日志
func (cluster *Cluster) waitCommitted(peer string, logIndex uint64) error {
	srcNodeConn, err := cluster.connections.BorrowPeerClient(peer)
	if err != nil {
		return err
	}
	defer cluster.connections.ReturnPeerClient(srcNodeConn)
	// 拿到源节点连接
	var peerIndex uint64
	for i := 0; i < 50; i++ {
		reply := srcNodeConn.Send(utils.ToCmdLine(getCommittedIndexCommand))
		switch reply := reply.(type) {
		case *protocol.IntReply:
			peerIndex = uint64(reply.Code)
			if peerIndex >= logIndex {
				return nil
			}

		case *protocol.StandardErrReply:
			logger.Infof("get committed index failed: %v", reply.Error())
		default:
			logger.Infof("get committed index unknown response: %+v", reply.ToBytes())
		}
		time.Sleep(time.Millisecond * 100)
	}
	return errors.New("wait committed timeout")
}

//	在迁移完成后，由 Leader 执行以更新集群路由。必须确保源节点和目标节点都已提交该日志后，才返回 OK
//
// 命令格式：cluster.migration.changeroute <taskId>
func execMigrationChangeRoute(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(migrationChangeRouteCommand)
	}
	// 检查是否为主节点，不是交由主节点处理
	state := cluster.raftNode.State()
	if state != raft.Leader {
		leaderConn, err := cluster.BorrowLeaderClient()
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		defer cluster.connections.ReturnPeerClient(leaderConn)
		return leaderConn.Send(cmdLine)
	}

	// 获取迁移任务（已经是主节点）
	taskID := string(cmdLine[1])
	logger.Infof("change router for migration %s, got task info", taskID)

	task := cluster.raftNode.FSM.GetMigratingTask(taskID)
	if task == nil {
		return protocol.MakeErrReply("ERR task not found")
	}
	logger.Infof("change route for migration: %s", taskID)

	// 提交 raft 日志，通知所有节点更新槽位归属
	logIndex, err := cluster.raftNode.Propose(&raft.LogEntry{
		Event:         raft.EventFinishMigrate,
		MigratingTask: task,
	})
	if err != nil {
		return protocol.MakeErrReply("ERR" + err.Error())
	}
	logger.Infof("change route for migration %s, raft proposed", taskID)

	// 等待源节点确认已提交该日志
	err = cluster.waitCommitted(task.SrcNode, logIndex)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	logger.Infof("change route for migration %s, confirm source node finished", taskID)

	// 等待目标节点确认已提交该日志
	err = cluster.waitCommitted(task.TargetNode, logIndex)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	logger.Infof("change route for migration %s, confirm target node finished", taskID)

	return protocol.MakeOkReply()
}

func init() {
	RegisterCmd(joinClusterCommand, execJoin)
	RegisterCmd(migrationChangeRouteCommand, execMigrationChangeRoute)
}
