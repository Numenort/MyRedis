package config

import (
	"myredis/lib/utils"
	"strconv"
	"time"
)

const (
	ClusterMode    = "cluster"    // 集群模式
	StandaloneMode = "standalone" // 单机模式
)

type ServerProperties struct {
	RunID string `cfg:"runid"` // 服务器唯一标识
	Bind  string `cfg:"bind"`  // 监听地址
	Port  int    `cfg:"port"`  // 监听端口

	Dir               string `cfg:"dir"`                  // 数据持久化目录
	AnnounceHost      string `cfg:"announce-host"`        // 对外公布的主机地址
	Databases         int    `cfg:"databases"`            // 数据库数量
	AppendOnly        bool   `cfg:"appendonly"`           // 是否开启 AOF 持久化
	AppendFilename    string `cfg:"appendfilename"`       // AOF 文件名
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"` // AOF 是否使用 RDB 前导
	AppendFsync       string `cfg:"appendfsync"`
	RequirePass       string `cfg:"requirepass"`
	RDBFilename       string `cfg:"rdbfilename"`

	MasterAuth        string `cfg:"masterauth"` // 主从复制时连接主节点的密码
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`

	ClusterEnable     bool   `cfg:"cluster-enable"`         // 是否启用集群模式
	ClusterAsSeed     bool   `cfg:"cluster-as-seed"`        // 是否作为种子节点
	ClusterSeed       string `cfg:"cluster-seed"`           // 种子节点地址列表
	RaftListenAddr    string `cfg:"raft-listen-addr"`       // Raft 协议监听地址（内部通信）
	RaftAdvertiseAddr string `cfg:"raft-advertise-address"` // Raft 对外公布的地址
	MasterInCluster   string `cfg:"master-in-cluster"`      // 当前实例在集群中所属的主节点地址

	CfgPath string `cfg:"cf, omitempty"`
}

type ServerInfo struct {
	StartUpTime time.Time
}

var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

// 返回当前实例对外公布的地址
func (p *ServerProperties) AnnounceAddress() string {
	if p.AnnounceHost != "" {
		return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
	}
	return p.Bind + ":" + strconv.Itoa(p.Port)
}

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}

func init() {
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       2025,
		AppendOnly: false,
		RunID:      utils.RandString(10),
	}
}
