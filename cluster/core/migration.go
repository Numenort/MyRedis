/* 负责处理集群中 Slot 的导出（export）、导入（import）和状态管理

函数说明：
- init()                          : 注册迁移相关命令及其执行函数
- (*slotStatus) startExporting() : 标记 Slot 进入导出状态，生成快照和 dirtyKeys 集合
- (*slotStatus) finishExportingWithinLock() : 完成导出，恢复 Slot 为托管状态
- (*Cluster) injectInsertCallback()  : 注册键插入回调，记录导出期间的脏键
- (*Cluster) injectDeleteCallback()  : 注册键删除回调，确保删除操作也被记录为脏键
- (*Cluster) dumpDataThroughConnection(c, keyset) : 将 key 集合以 AOF 命令形式通过连接发送
- execExport(cluster, c, cmdLine)    : 处理 export 命令，向对端发送全量数据
- execFinishExport(cluster, c, cmdLine) : 处理 migration.done 命令，发送增量数据并通知路由变更
- execStartMigration(cluster, c, cmdLine) : 处理 start 命令，启动后台导入任务
- (*Cluster) doImports(task)        : 执行两阶段数据导入：先拉全量，再拉增量，重建本地数据
*/

package core

import (
	"fmt"
	"myredis/aof"
	"myredis/cluster/raft"
	"myredis/datastruct/set"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/protocol"
	"strconv"
	"time"
)

const exportCommand = "cluster.migration.export"
const startMigrationCommand = "cluster.migration.start"
const migrationDoneCommand = "cluster.migration.done"

// 当前 slot 节点进入 exporting 状态
func (sm *slotStatus) startExporting() protocol.ErrorReply {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state != slotStateHosting {
		return protocol.MakeErrReply("Slot host is not in hosting state")
	}
	sm.state = slotStateExporting
	sm.dirtyKeys = set.Make()
	sm.exportSnapshot = sm.keys.ShallowCopy()
	return nil
}

// 完成导出流程，重置状态（需持有互斥锁）
func (sm *slotStatus) finishExportingWithinLock() {
	sm.state = slotStateHosting
	sm.dirtyKeys = nil
	sm.exportSnapshot = nil
}

// 数据库插入 key 时的回调函数：
// 获取对应的槽位，并且将 key 存入槽位，同时检测是否为脏键
func (cluster *Cluster) injectInsertCallback() {
	callback := func(dbIndex int, key string, entity *database.DataEntity) {
		// 获取相应的槽（不存在则创建）
		slotIndex := cluster.GetSlot(key)
		slotManager := cluster.slotsManager.getSlot(slotIndex)
		slotManager.mu.Lock()
		defer slotManager.mu.Unlock()
		// 将对应的 key 存入槽中
		slotManager.keys.Add(key)
		if slotManager.state == slotStateExporting {
			// 如果在导出状态中，新增的 key 为脏键
			slotManager.dirtyKeys.Add(key)
		}
	}
	cluster.db.SetKeyInsertedCallback(callback)
}

// 数据库删除 key 时的回调函数：
// 导出过程中删除操作也可能发生，这类 key 也视为 dirty key
func (cluster *Cluster) injectDeleteCallback() {
	callback := func(dbIndex int, key string, entity *database.DataEntity) {
		// 获取相应的槽
		slotIndex := cluster.GetSlot(key)
		slotManager := cluster.slotsManager.getSlot(slotIndex)
		slotManager.mu.Lock()
		defer slotManager.mu.Unlock()
		// 从槽中删除 key
		slotManager.keys.Remove(key)
		if slotManager.state == slotStateExporting {
			// 发送在导出状态中，键也是脏键
			slotManager.dirtyKeys.Add(key)
		}
	}
	cluster.db.SetKeyDeletedCallback(callback)
}

// 将指定 key 集合的数据通过 Redis 连接发送出去
// 参数：
//   - c: 输出连接（通常是 TCP 流向目标节点）
//   - keyset: 待导出的 key 集合（如 snapshot 或 dirtyKeys）
func (cluster *Cluster) dumpDataThroughConnection(conn myredis.Connection, keySet *set.Set) {
	keySet.ForEach(func(key string) bool {
		entity, ok := cluster.db.GetEntity(0, key)
		if ok {
			// 序列化为可执行命令并发送
			ret := aof.EntityToCmd(key, entity)
			_, _ = conn.Write(ret.ToBytes())
			// 如果存在过期时间
			expireTime := cluster.db.GetExpiration(0, key)
			if expireTime != nil {
				ret := aof.MakeExpiredCmd(key, *expireTime)
				_, _ = conn.Write(ret.ToBytes())
			}
		}
		return true
	})
}

// 处理导出命令：将指定迁移任务涉及的 Slot 数据全量导出到客户端连接
// 命令格式：cluster.migration.export <taskId>
// 流程：
//  1. 从 Raft FSM 获取迁移任务
//  2. 检查是否有其他导入任务正在进行
//  3. 对每个 slot 执行 startExporting 并发送 snapshot 数据
func execExport(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(exportCommand)
	}

	var task *raft.MigratingTask
	taskID := string(cmdLine[1])

	for i := 0; i < 50; i++ {
		// 尝试从 raft 状态机中获取迁移任务
		task = cluster.raftNode.FSM.GetMigratingTask(taskID)
		if task != nil {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	if task == nil {
		return protocol.MakeErrReply("ERR get migration task timeout")
	}

	// 检查是否已存在导入任务
	cluster.slotsManager.mu.Lock()
	if cluster.slotsManager.importingTask != nil {
		cluster.slotsManager.mu.Unlock()
		return protocol.MakeErrReply("ERR another migrating task in progress")
	}
	cluster.slotsManager.importingTask = task
	cluster.slotsManager.mu.Unlock()

	// 对任务的每一个槽位进行对应的迁移操作
	for _, slotID := range task.Slots {
		slotManager := cluster.slotsManager.getSlot(slotID)
		errReply := slotManager.startExporting()
		if errReply != nil {
			return errReply
		}
		// 发送该槽位对应的 key 集合数据
		cluster.dumpDataThroughConnection(c, slotManager.exportSnapshot)
		logger.Info("finish dump slot ", slotID)
	}
	c.Write(protocol.MakeOkReply().ToBytes())
	return &protocol.NoReply{}
}

// 完成导出阶段：接收并处理导出节点发来的脏键，通知集群更改路由
// 命令格式：cluster.migration.done <taskID>
func execFinishExport(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(exportCommand)
	}
	taskID := string(cmdLine[1])
	logger.Info("finish migration task: ", taskID)

	var task *raft.MigratingTask
	for i := 0; i < 50; i++ {
		task = cluster.raftNode.FSM.GetMigratingTask(taskID)
		if task != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if task == nil {
		return protocol.MakeErrReply("ERR get migrating task timeout")
	}
	logger.Info(fmt.Sprintf("finish migration task %s, got task info", taskID))

	// 按顺序释放加入的锁
	var lockedSlots []uint32
	defer func() {
		for i := len(lockedSlots) - 1; i >= 0; i-- {
			slotID := lockedSlots[i]
			slotManager := cluster.slotsManager.getSlot(slotID)
			slotManager.mu.Unlock()
		}
	}()

	for _, slotID := range task.Slots {
		slotManager := cluster.slotsManager.getSlot(slotID)
		// 加锁，防止新的脏数据进入
		slotManager.mu.Lock()
		lockedSlots = append(lockedSlots, slotID)
		// 发送脏数据
		cluster.dumpDataThroughConnection(c, slotManager.dirtyKeys)
		slotManager.finishExportingWithinLock()
	}
	logger.Infof("finish migration task %s, dirty keys sended", taskID)

	// 获取与主节点的连接
	leaderConn, err := cluster.BorrowLeaderClient()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer cluster.connections.ReturnPeerClient(leaderConn)
	// 发送路由更新命令
	reply := leaderConn.Send(utils.ToCmdLine(migrationChangeRouteCommand, taskID))
	switch reply := reply.(type) {
	case *protocol.StatusReply, *protocol.OkReply:
		return protocol.MakeOkReply()
	case *protocol.StandardErrReply:
		logger.Infof("migration done command failed: %s", reply.Error())
	default:
		logger.Infof("finish migration request unknown response %s", string(reply.ToBytes()))
	}

	logger.Infof("finish migration task %s, route changed", taskID)
	c.Write(protocol.MakeOkReply().ToBytes())
	return &protocol.NoReply{}
}

// 接收来自 leader 的迁移启动命令，开始导入数据
// 命令格式：cluster.migration.start <taskId> <srcNode> <slotId1> [slotId2...]
func execStartMigration(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 4 {
		return protocol.MakeArgNumErrReply(startMigrationCommand)
	}

	taskID := string(cmdLine[1])
	srcNode := string(cmdLine[2])
	var slotIDs []uint32

	// 读取所有的 slotID
	for _, slot := range cmdLine[3:] {
		slotStr := string(slot)
		slotID, err := strconv.Atoi(slotStr)
		if err != nil {
			return protocol.MakeErrReply("illegal slot id: " + strconv.Itoa(slotID))
		}
		slotIDs = append(slotIDs, uint32(slotID))
	}
	// 封装成任务
	task := &raft.MigratingTask{
		ID:         taskID,
		SrcNode:    srcNode,
		TargetNode: cluster.SelfID(),
		Slots:      slotIDs,
	}
	cluster.slotsManager.mu.Lock()
	cluster.slotsManager.importingTask = task
	cluster.slotsManager.mu.Unlock()

	logger.Infof("received importing task %s, %d slots to import", task.ID, len(task.Slots))
	// 执行导入命令
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("panic in doImports: %v", e)
			}
		}()
		cluster.doImports(task)
	}()
	return protocol.MakeOkReply()
}

// 功能函数，用于某个节点实际执行导入命令，分为两个阶段：
//
//	Phase 1: 从源节点拉取全量数据（snapshot）
//	Phase 2: 拉取增量 dirty keys 并最终确认迁移完成
func (cluster *Cluster) doImports(task *raft.MigratingTask) error {
	// 发起全量导出请求，对方执行 execExport函数，返回相关命令 / OK标记
	cmdLine := utils.ToCmdLine(exportCommand, task.ID)
	stream, err := cluster.connections.NewStream(task.SrcNode, cmdLine)
	if err != nil {
		return err
	}
	defer stream.Close()

	simpleConn := connection.NewSimpleConn()
recvLoop:
	// 持续解析返回结果
	for protoc := range stream.Stream() {
		if protoc.Err != nil {
			return fmt.Errorf("expect error: %v", protoc.Err)
		}
		switch reply := protoc.Data.(type) {
		case *protocol.MultiBulkReply:
			// 执行实际的数据相关命令，重建该槽位对应的数据
			_ = cluster.db.Exec(simpleConn, reply.Args)
		case *protocol.StatusReply, *protocol.OkReply:
			if protocol.IsOKReply(reply) {
				logger.Info("importing task received OK reply")
				break recvLoop
			} else {
				msg := fmt.Sprintf("migrate error: %s", string(reply.ToBytes()))
				logger.Error(msg)
				return protocol.MakeErrReply(msg)
			}
		case protocol.ErrorReply:
			msg := fmt.Sprintf("migrate error: %s", reply.Error())
			logger.Error(msg)
			return protocol.MakeErrReply(msg)
		}
	}
	// 请求源节点发送增量 dirty keys
	stream2, err := cluster.connections.NewStream(task.SrcNode, utils.ToCmdLine(migrationDoneCommand, task.ID))
	if err != nil {
		return err
	}
	defer stream2.Close()

recvLoop2:
	for protoc := range stream2.Stream() {
		if protoc.Err != nil {
			return fmt.Errorf("export error: %v", protoc.Err)
		}
		switch reply := protoc.Data.(type) {
		case *protocol.MultiBulkReply:
			// 接收数据相关命令，后面修改的脏数据也写入
			_ = cluster.db.Exec(simpleConn, reply.Args)
		case *protocol.StatusReply, *protocol.OkReply:
			if protocol.IsOKReply(reply) {
				logger.Info("importing task received OK reply")
				break recvLoop2
			} else {
				msg := fmt.Sprintf("migrate error: %s", string(reply.ToBytes()))
				logger.Error(msg)
				return protocol.MakeErrReply(msg)
			}
		case protocol.ErrorReply:
			msg := fmt.Sprintf("migrate error: %s", reply.Error())
			logger.Error(msg)
			return protocol.MakeErrReply(msg)
		}
	}

	return nil
}

func init() {
	RegisterCmd(exportCommand, execExport)
	RegisterCmd(migrationDoneCommand, execFinishExport)
	RegisterCmd(startMigrationCommand, execStartMigration)
}
