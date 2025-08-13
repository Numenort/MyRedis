// 负责处理集群中 Slot 的导出（export）、导入（import）和状态管理

package core

import (
	"myredis/aof"
	"myredis/cluster/raft"
	"myredis/datastruct/set"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/protocol"
)

const startMigrationCommand = "cluster.migration.start"
const exportCommand = "cluster.migration.export"

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
// 返回：
//   - OK + NoReply：数据已开始推送（通过连接流式传输）
//   - ErrReply：参数错误 / 任务不存在 / 状态冲突
func execExport(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(exportCommand)
	}

	var task *raft.MigratingTask
	taskID := string(cmdLine[1])

}
