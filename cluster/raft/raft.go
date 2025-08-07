package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type RaftConfig struct {
	RedisAdvertiseAddr string // 对外服务地址，也是 Raft 节点 ID
	RaftListenAddr     string // Raft 内部通信监听地址
	RaftAdvertiseAddr  string // Raft 对外广播地址
	Dir                string // 数据存储目录
}

// 返回 Raft 节点 ID
func (cfg *RaftConfig) ID() string {
	return cfg.RedisAdvertiseAddr
}

// 一个完整的 Raft 节点实例
type Node struct {
	Cfg           *RaftConfig
	FSM           *FSM
	inner         *raft.Raft         // Raft 实例
	logStore      raft.LogStore      // 日志存储
	stableStore   raft.StableStore   // 持久化存储
	SnapshotStore raft.SnapshotStore // 快照存储
	transport     raft.Transport     // 网络传输层
	watcher       watcher            // 监听器，用于检测主从切换
}

// 用于监听主从切换事件
type watcher struct {
	watch         func(*FSM)             // 定期检查 FSM 状态
	currentMaster string                 // 当前主节点，用于检测主节点变化
	onFailover    func(newMaster string) // 主节点变更时的回调函数
}

// 启动一个新的 Raft 节点，初始化网络、存储、FSM，并创建 raft.Raft 实例
func StartNode(cfg *RaftConfig) (*Node, error) {
	// 没有设置广播地址，默认使用监听地址
	if cfg.RaftAdvertiseAddr == "" {
		cfg.RaftAdvertiseAddr = cfg.RaftListenAddr
	}

	// 设置节点 ID
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(cfg.ID())
	if config.LocalID == "" {
		return nil, errors.New("redis address is required")
	}
	// 设置变更通知通道
	leaderNotifyCh := make(chan bool, 10)
	config.NotifyCh = leaderNotifyCh

	// 解析广播地址
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertiseAddr)
	if err != nil {
		return nil, err
	}
	// 创建 TCP 传输层，负责节点间通信
	transport, err := raft.NewTCPTransport(cfg.RaftListenAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	// 创建快照存储器：将 FSM 快照保存到磁盘
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.Dir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}
	// 创建持久化存储器
	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.Dir, "raft.db"), // 数据库存放路径
	})
	if err != nil {
		return nil, err
	}

	// 初始化状态机
	fsm := &FSM{
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}

	// 创建两个存储
	logStore := boltDB
	stableStore := boltDB

	// 创建 Raft 核心实例
	inner, err := raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return nil, err
	}
	// 构造 Node 对象，同时设置状态监听器检测主从切换
	node := &Node{
		Cfg:           cfg,
		FSM:           fsm,
		inner:         inner,
		logStore:      logStore,
		stableStore:   stableStore,
		SnapshotStore: snapshotStore,
		transport:     transport,
	}
	node.setupWatch()
	return node, nil
}

// 设置 FSM 状态变更的监听器
func (node *Node) setupWatch() {
	node.watcher.watch = func(fsm *FSM) {
		newMaster := fsm.SlaveMasters[node.Self()]
		// 当前主节点与监控时不一致
		if newMaster != node.watcher.currentMaster {
			node.watcher.currentMaster = newMaster
			if node.watcher.onFailover != nil {
				node.watcher.onFailover(newMaster)
			}
		}
	}
}

// 设置主节点变更时的回调函数
func (node *Node) SetOnFailover(fn func(newMaster string)) {
	// 监控当前主节点
	node.watcher.currentMaster = node.FSM.getMaster(node.Self())
	// 设置回调函数
	node.watcher.onFailover = fn
}

// 检查该节点是否已经有 Raft 状态数据（日志、快照、元数据）
// 用于判断是首次启动 / 重新启动
func (node *Node) HasExistingState() (bool, error) {
	return raft.HasExistingState(node.logStore, node.stableStore, node.SnapshotStore)
}

// 初始化一个全新的 Raft 集群，只有第一个节点可以调用
func (node *Node) BootstrapCluster(slotCount int) error {
	future := node.inner.BootstrapCluster(
		raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(node.Cfg.ID()), // 节点 ID
					Address: node.transport.LocalAddr(),   // 广播地址
				},
			},
		},
	)
	err := future.Error()
	if err != nil {
		return fmt.Errorf("BootstrapCluster failed: %v", err)
	}

	// 等待该节点成为 leader
	for {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 提交初始化任务：把所有 slot 分配给自己
	_, err = node.Propose(
		&LogEntry{
			Event: EventSeedStart,
			InitTask: &InitTask{
				Leader:    node.Self(),
				SlotCount: slotCount,
			},
		},
	)
	return err
}

// 安全关闭 Raft 节点，通知其他节点自己下线，并释放资源
func (node *Node) Close() error {
	// 关闭节点（异步操作）
	future := node.inner.Shutdown()
	return fmt.Errorf("raft shutdown %v", future.Error())
}

// 提交命令到 raft 日志，需要输入参数 event: 要执行的操作（如迁移、故障转移等）
//
//	返回值：
//	index: 日志索引（可用于等待日志被应用）
//	error: 提交失败原因（如不是 leader）
func (node *Node) Propose(event *LogEntry) (uint64, error) {
	stream, err := json.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("marshal event failed: %v", err)
	}
	// 提交日志（异步操作）
	future := node.inner.Apply(stream, 0)
	err = future.Error()
	if err != nil {
		return 0, fmt.Errorf("raft propose failed: %v", err)
	}
	return future.Index(), nil
}

// 将一个新的 Redis 节点加入 Raft 集群，必须由当前 leader 调用
//
//	参数：
//	redisAddr: 新节点的 Redis 地址（也是其 Raft ID）
//	raftAddr:  新节点的 Raft 通信地址
func (node *Node) AddToRaft(redisAddr, raftAddr string) error {
	// 获取当前集群配置
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}

	// 检测当前节点是否已加入集群
	id := raft.ServerID(redisAddr)
	for _, serve := range configFuture.Configuration().Servers {
		if serve.ID == id {
			return errors.New("already in cluster")
		}
	}
	// 添加到投票者，可参与选举和复制日志
	future := node.inner.AddVoter(id, raft.ServerAddress(raftAddr), 0, 0)
	return future.Error()
}

// 从 Raft 集群中移除某个节点
//
// 参数 redisAddr: 要移除的节点的 Redis 地址（即 Raft ID）
func (node *Node) HandleEvict(redisAddr string) error {
	// 获取集群配置
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}

	id := raft.ServerID(redisAddr)
	// 移除对应 id
	for _, server := range configFuture.Configuration().Servers {
		if server.ID == id {
			err := node.inner.RemoveServer(id, 0, 0).Error()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
