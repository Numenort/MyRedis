package core

import (
	rdbcore "github.com/hdt3213/rdb/core"
	"myredis/cluster/raft"
	dbimpl "myredis/database"
	"myredis/datastruct/set"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
	"sync"
)

// Cluster 代表当前节点在分布式集群中的本地视图和状态管理
type Cluster struct {
	raftNode    *raft.Node
	db          database.DBEngine
	connections ConnectionFactory // 客户端与节点间连接工厂
	config      *Config

	slotsManager    *slotsManage        // 管理当前节点上所有 slot 的状态
	rebalanceManger *rebalanceManager   // 负责处理 slot 迁移任务
	transactions    *TransactionManager // TCC 分布式事务管理器
	replicaManager  *replicaManager     // 主从复制状态管理

	closeChan chan struct{}

	getSlotImpl  func(key string) uint32    // 将 key 映射为 slot ID
	pickNodeImpl func(slotID uint32) string // 根据 slot 选择目标节点 ID
	id_          string
}

type Config struct {
	raft.RaftConfig
	StartAsSeed     bool
	JoinAddress     string // 要加入的现有集群地址
	Master          string
	connectionSthub ConnectionFactory // 测试用连接工厂 stub
	noCron          bool
}

type slotsManage struct {
	mu            *sync.RWMutex
	slots         map[uint32]*slotStatus
	importingTask *raft.MigratingTask
}

const (
	slotStateHosting = iota
	slotStateImporting
	slotStateExporting
)

type slotStatus struct {
	mu    *sync.RWMutex
	state int
	keys  *set.Set // 当前 Slot 管理的所有 key 的集合

	exportSnapshot *set.Set // 导出开始时的 keys 快照
	dirtyKeys      *set.Set // 导出过程中被插入或删除的 key 集合
}

func (ssm *slotsManage) getSlot(index uint32) *slotStatus {
	ssm.mu.RLock()
	slot := ssm.slots[index]
	ssm.mu.RUnlock()
	// 尝试获取 slot
	if slot != nil {
		return slot
	}
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	// 双重检查，防止被修改
	slot = ssm.slots[index]
	if slot != nil {
		return slot
	}
	slot = &slotStatus{
		state: slotStateHosting,
		keys:  set.Make(),
		mu:    &sync.RWMutex{},
	}
	ssm.slots[index] = slot
	return slot
}

func newSlotsManager() *slotsManage {
	return &slotsManage{
		mu:    &sync.RWMutex{},
		slots: map[uint32]*slotStatus{},
	}
}

func NewCluster(cfg *Config) (*Cluster, error) {
	var connections ConnectionFactory
	if cfg.connectionSthub != nil {
		connections = cfg.connectionSthub
	} else {
		connections = newDefaultClientFactory()
	}

	// 初始化本地数据库
	db := dbimpl.NewStandaloneServer()

	// 启动 Raft 节点
	raftNode, err := raft.StartNode(&cfg.RaftConfig)
	if err != nil {
		return nil, err
	}
	// 检测节点状态
	hasState, err := raftNode.HasExistingState()
	if err != nil {
		return nil, err
	}
	if !hasState {
		// 初次启动
		if cfg.StartAsSeed {
			// 作为种子节点启动
			err = raftNode.BootstrapCluster(SlotCount)
			if err != nil {
				return nil, err
			}
		} else {
			// 加入集群
			conn, err := connections.BorrowPeerClient(cfg.JoinAddress)
			if err != nil {
				return nil, err
			}
			defer connections.ReturnPeerClient(conn)
			// 构造 join 命令：JOIN <redis_addr> <raft_addr> [master_id]
			JoinCmdLine := utils.ToCmdLine(joinClusterCommand, cfg.RedisAdvertiseAddr, cfg.RaftListenAddr)
			if cfg.Master != "" {
				JoinCmdLine = append(JoinCmdLine, []byte(cfg.Master))
			}
			res := conn.Send(JoinCmdLine)
			if err := protocol.Try2ErrorReply(res); err != nil {
				return nil, err
			}
		}
	}

	cluster := &Cluster{
		raftNode:        raftNode,
		db:              db,
		connections:     connections,
		config:          cfg,
		rebalanceManger: newRebalanceManager(),
		slotsManager:    newSlotsManager(),
		transactions:    newTransactionManager(),
		replicaManager:  newReplicaManager(),
		closeChan:       make(chan struct{}),
	}
	cluster.pickNodeImpl = func(slotID uint32) string {
		return defaultPickNodeImpl(cluster, slotID)
	}
	cluster.getSlotImpl = func(key string) uint32 {
		return defaultGetSlotImpl(cluster, key)
	}
	// 注册回调函数
	cluster.injectInsertCallback()
	cluster.injectDeleteCallback()
	cluster.registerOnFailover()
	// 周期性执行任务
	go cluster.clusterCron()
	return cluster, nil
}

func (cluster *Cluster) AfterClientClose(c myredis.Connection) {

}

// 关闭集群节点，释放所有资源
func (cluster *Cluster) Close() {
	close(cluster.closeChan)
	cluster.db.Close()
	err := cluster.raftNode.Close()
	if err != nil {
		panic(err)
	}
}

// 加载 RDB 快照文件到本地数据库
func (cluster *Cluster) LoadRDB(dec *rdbcore.Decoder) error {
	return cluster.db.LoadRDB(dec)
}

// 返回节点对应 ID
func (cluster *Cluster) SelfID() string {
	if cluster.raftNode == nil {
		return cluster.id_
	}
	return cluster.raftNode.Cfg.ID()
}
