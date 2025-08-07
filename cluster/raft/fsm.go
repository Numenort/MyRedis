// fsm.go 实现了一个基于 Hashicorp Raft 算法的分布式状态机（FSM），
// 用于管理 Redis 集群的元数据，包括：
//
//   - 槽位（slot）到节点的映射（用于请求路由）
//   - 主从拓扑关系（Master-Slave）
//   - 槽迁移任务（MigratingTask）
//   - 故障转移任务（FailoverTask）
//
// 所有状态变更通过 Raft 日志复制，确保多个协调节点之间的一致性。
// 状态机（FSM）是 Raft 的核心组件之一，负责：
//
//   1. Apply：应用已提交的日志条目，更新本地状态
//   2. Snapshot：生成快照，减少日志回放时间
//   3. Restore：从快照恢复状态
//
// 本实现支持集群初始化、节点加入、槽迁移、主从切换等操作。
//
// 设计原则：
//   - 状态变更只能通过 Raft 日志进行（强一致性）
//   - 所有字段由读写锁保护（并发安全）
//   - 快照中只保存核心状态，派生字段在恢复时重建

package raft

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type FSM struct {
	mu           sync.RWMutex
	Node2Slot    map[string][]uint32       // 节点 -> 槽位
	Slot2Node    map[uint32]string         // 槽位 -> 节点
	Migratings   map[string]*MigratingTask // 迁移任务
	MasterSlaves map[string]*MasterSlave   // 主节点 -> 主从关系
	SlaveMasters map[string]string         // 从节点 -> 主节点
	Failovers    map[string]*FailoverTask  // 故障转移任务
	changed      func(*FSM)                // 状态变化回调函数
}

// 表示一个正在进行的槽位迁移任务，不可变
type MigratingTask struct {
	ID         string
	SrcNode    string   // 负责槽位管理的源节点
	TargetNode string   // 负责接收槽位的目标节点
	Slots      []uint32 //需要迁移的槽位
}

// 表示节点属性，包含主节点及其从节点列表
// 主节点为 ""，即是一个从节点
type MasterSlave struct {
	MasterID string
	Slaves   []string
}

// 一次主节点切换任务
type FailoverTask struct {
	ID          string // 任务 ID
	OldMasterID string
	NewMasterID string
}

// 初始化集群时，将所有槽位分配给初始主节点
type InitTask struct {
	Leader    string
	SlotCount int
}

// 一个新节点加入集群（主或者从）
type JoinTask struct {
	NodeID string // 节点 ID
	Master string // 主节点 ID
}

// Raft 日志事件类型

const (
	EventStartMigrate   = iota + 1 // 开始迁移槽位
	EventFinishMigrate             // 完成迁移槽位
	EventSeedStart                 // 初始化集群
	EventStartFailover             // 开始故障转移
	EventFinishFailover            // 完成故障转移
	EventJoin                      // 新节点加入
)

// Raft 日志条目，记录集群状态的变更
type LogEntry struct {
	Event         int            `json:"event"` // 事件类型
	MigratingTask *MigratingTask `json:"migrating_task,omitempty"`
	InitTask      *InitTask      `json:"init_task,omitempty"`
	FailoverTask  *FailoverTask  `json:"failover_task,omitempty"`
	JoinTask      *JoinTask      `json:"join_task,omitempty"`
}

// Apply 在 Raft 日志被多数节点提交后调用
func (fsm *FSM) Apply(log *raft.Log) interface{} {
	// 创建日志
	entry := &LogEntry{}
	err := json.Unmarshal(log.Data, entry)
	if err != nil {
		panic(err)
	}
	// 保护 FSM 结构体的共享字段
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	switch entry.Event {
	case EventStartMigrate:
		task := entry.MigratingTask
		fsm.Migratings[task.ID] = task

	case EventFinishMigrate:
		task := entry.MigratingTask
		delete(fsm.Migratings, task.ID) // 删除任务
		fsm.addSlots(task.TargetNode, task.Slots)
		fsm.removeSlots(task.SrcNode, task.Slots)

	case EventSeedStart: // 初始化集群
		// 创建槽位并设置主节点
		slots := make([]uint32, int(entry.InitTask.SlotCount))
		for i := 0; i < entry.InitTask.SlotCount; i++ {
			fsm.Slot2Node[uint32(i)] = entry.InitTask.Leader
			slots[i] = uint32(i)
		}
		fsm.Node2Slot[entry.InitTask.Leader] = slots
		fsm.addNode(entry.InitTask.Leader, "")

	case EventStartFailover: // 故障转移开始
		task := entry.FailoverTask
		fsm.Failovers[task.ID] = task

	case EventFinishFailover: // 完成故障转移
		task := entry.FailoverTask
		fsm.failover(task.OldMasterID, task.NewMasterID)
		// 获取旧的主节点的槽位
		slots := fsm.Node2Slot[task.OldMasterID]
		fsm.addSlots(task.NewMasterID, slots)
		fsm.removeSlots(task.OldMasterID, slots)
		// 移除对应任务
		delete(fsm.Failovers, task.ID)

	case EventJoin:
		task := entry.JoinTask
		fsm.addNode(task.NodeID, task.Master)
	}
	// 如果存在状态回调函数
	if fsm.changed != nil {
		fsm.changed(fsm)
	}
	return nil
}

// 表示某一时刻 FSM 的状态快照，用于快速恢复，避免重放大量历史日志
// 只需下列三个成员，其余可以通过映射得到
type FSMSnapshot struct {
	Slot2Node    map[uint32]string
	Migratings   map[string]*MigratingTask
	MasterSlaves map[string]*MasterSlave
}

// 将快照写入磁盘
func (snapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// 序列化为字节流
		data, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		// 写入字节流
		_, err = sink.Write(data)
		if err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Close() // 出错则取消写入
	}
	return err
}

func (snapshot *FSMSnapshot) Release() {}

// 生成当前 FSM 的快照对象，用于 Raft 在适当时机调用它
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	slot2Node := make(map[uint32]string)
	for key, val := range fsm.Slot2Node {
		slot2Node[key] = val
	}
	migratings := make(map[string]*MigratingTask)
	for k, v := range fsm.Migratings {
		migratings[k] = v
	}
	masterSlaves := make(map[string]*MasterSlave)
	for k, v := range fsm.MasterSlaves {
		masterSlaves[k] = v
	}
	return &FSMSnapshot{
		Slot2Node:    slot2Node,
		Migratings:   migratings,
		MasterSlaves: masterSlaves,
	}, nil
}

// 从快照中恢复 FSM 状态，在节点重启或新节点加入时可能被调用
func (fsm *FSM) Restore(src io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	// 读取快照数据
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	// 反序列化为快照对象
	snapshot := &FSMSnapshot{}
	err = json.Unmarshal(data, snapshot)
	if err != nil {
		return err
	}
	// 恢复核心映射
	fsm.Slot2Node = snapshot.Slot2Node
	fsm.Migratings = snapshot.Migratings
	fsm.MasterSlaves = snapshot.MasterSlaves
	// 重建剩余成员
	fsm.Node2Slot = make(map[string][]uint32)
	for slot, node := range snapshot.Slot2Node {
		fsm.Node2Slot[node] = append(fsm.Node2Slot[node], slot)
	}
	for master, slaves := range snapshot.MasterSlaves {
		for _, slave := range slaves.Slaves {
			fsm.SlaveMasters[slave] = master
		}
	}
	// 触发变更通知
	if fsm.changed != nil {
		fsm.changed(fsm)
	}
	return nil
}
