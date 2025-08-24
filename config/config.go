package config

import (
	"myredis/lib/utils"
	"time"
)

const (
	ClusterMode    = "cluster"    // 集群
	StandaloneMode = "standalone" // 单个实例
)

type ServerProperties struct {
	RunID string `cfg:"runid"`
	Bind  string `cfg:"bind"`
	Port  int    `cfg:"port"`

	Dir               string `cfg:"dir"`
	Databases         int    `cfg:"databases"`
	AppendOnly        bool   `cfg:"appendonly"`
	AppendFilename    string `cfg:"appendfilename"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	AppendFsync       string `cfg:"appendfsync"`
	RequirePass       string `cfg:"requirepass"`
	RDBFilename       string `cfg:"rdbfilename"`

	MasterAuth        string `cfg:"masterauth"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`

	ClusterEnable bool `cfg:"cluster-enable"`

	CfgPath string `cfg:"cf, omitempty"`
}

type ServerInfo struct {
	StartUpTime time.Time
}

var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       2025,
		AppendOnly: false,
		RunID:      utils.RandString(10),
	}
}

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
