package config

import "time"

const (
	ClusterMode    = "cluster"    // 集群
	StandaloneMode = "standalone" // 单个实例
)

type ServerProperties struct {
	RunID string `cfg:"runid"`
	Port  int    `cfg:"port"`

	Dir               string `cfg:"dir"`
	Databases         int    `cfg:"databases"`
	AppendOnly        bool   `cfg:"appendonly"`
	AppendFilename    string `cfg:"appendfilename"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	AppendFsync       string `cfg:"appendfsync"`
	RequirePass       string `cfg:"requirepass"`
	RDBFilename       string `cfg:"rdbfilename"`

	ClusterEnable bool `cfg:"cluster-enable"`

	CfgPath string `cfg:"cf, omitempty"`
}

type ServerInfo struct {
	StartUpTime time.Time
}

var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}
