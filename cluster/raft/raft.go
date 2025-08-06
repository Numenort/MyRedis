package raft

import "github.com/hashicorp/raft"

type RaftConfig struct {
	RedisAdvertiseAddr string // 对外服务地址
	RaftListenAddr     string // Raft 内部通信监听地址
	RaftAdvertiseAddr  string // Raft 广播地址
	Dir                string // 数据存储目录
}

func (cfg *RaftConfig) ID() string {
	return cfg.RedisAdvertiseAddr
}

// 完整的 Raft 节点实例
type Node struct {
	cfg           *RaftConfig
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
	watch         func(*FSM) // 定期检查 FSM 状态
	currentMaster string
	onFailover    func(newMaster string)
}
