package config

import (
	"bufio"
	"io"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
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
	Dir   string `cfg:"dir"`   // 数据持久化目录

	AnnounceHost      string `cfg:"announce-host"`        // 对外公布的主机地址
	AppendOnly        bool   `cfg:"appendonly"`           // 是否开启 AOF 持久化
	AppendFilename    string `cfg:"appendfilename"`       // AOF 文件名
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"` // AOF 是否使用 RDB 前导
	AppendFsync       string `cfg:"appendfsync"`          // 持久化机制
	Databases         int    `cfg:"databases"`            // 数据库数量
	RequirePass       string `cfg:"requirepass"`
	RDBFilename       string `cfg:"rdbfilename"`
	MaxClients        int    `cfg:"maxclients"`

	MasterAuth        string `cfg:"masterauth"` // 主从复制时连接主节点的密码
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`

	ClusterEnable     bool   `cfg:"cluster-enable"`         // 是否启用集群模式
	ClusterAsSeed     bool   `cfg:"cluster-as-seed"`        // 是否作为种子节点
	ClusterSeed       string `cfg:"cluster-seed"`           // 种子节点地址列表
	RaftListenAddr    string `cfg:"raft-listen-address"`    // Raft 协议监听地址（内部通信）
	RaftAdvertiseAddr string `cfg:"raft-advertise-address"` // Raft 对外公布的地址
	MasterInCluster   string `cfg:"master-in-cluster"`      // 当前实例在集群中所属的主节点地址

	CfgPath string `cfg:"cf, omitempty"`
}

type ServerInfo struct {
	StartUpTime time.Time
}

var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

// 从输入流中解析配置文件
func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	raw := make(map[string]string)
	scanner := bufio.NewScanner(src)
	// 存储 <配置：值> 键值对
	for scanner.Scan() {
		line := scanner.Text()
		// 跳过注释和空格
		if len(line) > 0 && strings.TrimLeft(line, " ")[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 {
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			raw[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	t := reflect.TypeOf(config)  // 获取类型字段
	v := reflect.ValueOf(config) // 获取值字段
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		// 获取类型以及对应的值
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimSpace(key) == "" {
			// 没有 cfg 标签
			key = field.Name
		}
		key = strings.ToLower(key)
		value, exists := raw[key]
		if !exists {
			continue
		}
		// 根据字段类型设置 config 字段值
		switch field.Type.Kind() {
		case reflect.String:
			fieldVal.SetString(value)
		case reflect.Int:
			intValue, err := strconv.ParseInt(value, 10, 64)
			if err == nil {
				fieldVal.SetInt(intValue)
			}
		case reflect.Bool:
			boolValue := strings.ToLower(value) == "yes" ||
				strings.ToLower(value) == "true" ||
				strings.ToLower(value) == "1"
			fieldVal.SetBool(boolValue)
		case reflect.Slice:
			if field.Type.Elem().Kind() == reflect.String {
				slice := strings.Split(value, ",")
				fieldVal.Set(reflect.ValueOf(slice))
			}
		}
	}
	return config
}

// 从指定的配置文件路径加载配置，并更新全局配置
func SetupConfig(configFilename string) {
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// 解析文件
	Properties = parse(file)
	Properties.RunID = utils.RandString(40)
	// 记录配置文件的绝对路径
	configFilePath, err := filepath.Abs(configFilename)
	if err != nil {
		return
	}
	Properties.CfgPath = configFilePath
	if Properties.Dir == "" {
		// 使用当前目录
		Properties.Dir = "."
	}
}

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
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       2025,
		AppendOnly: false,
		RunID:      utils.RandString(40),
	}
}
