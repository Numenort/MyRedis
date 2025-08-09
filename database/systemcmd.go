package database

import (
	"fmt"
	"myredis/config"
	"myredis/interface/myredis"
	"myredis/protocol"
	"os"
	"runtime"
	"strings"
	"time"
)

// Ping 处理 PING 命令：无参数返回 PONG，一个参数则回显该参数，否则报错。
func Ping(c myredis.Connection, args [][]byte) myredis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// Auth 处理 AUTH 命令：校验密码是否正确。服务器未设密码或密码错误时返回错误。
func Auth(c myredis.Connection, args [][]byte) myredis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	// 服务器未配置密码
	if config.Properties.RequirePass == "" {
		return protocol.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	password := string(args[0])
	c.SetPassword(password) // 记录客户端提交的密码
	if config.Properties.RequirePass != password {
		return protocol.MakeErrReply("ERR invalid password")
	}
	return &protocol.OkReply{}
}

func isAuthenticated(c myredis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func Dbsize(c myredis.Connection, db *Server) myredis.Reply {
	keys, _ := db.GetDBSize(c.GetDBIndex())
	return protocol.MakeIntReply(int64(keys))
}

func Info(db *Server, args [][]byte) myredis.Reply {
	if len(args) == 0 {
		infoCommandList := [...]string{"server", "client", "cluster", "keyspace"}
		var allSection []byte
		for _, infoCommand := range infoCommandList {
			allSection = append(allSection, GenMydisInfoString(infoCommand, db)...)
		}
		return protocol.MakeBulkReply(allSection)
	} else if len(args) == 1 {
		section := strings.ToLower(string(args[0]))
		switch section {
		case "server":
			reply := GenMydisInfoString("server", db)
			return protocol.MakeBulkReply(reply)
		case "client":
			reply := GenMydisInfoString("client", db)
			return protocol.MakeBulkReply(reply)
		case "cluster":
			reply := GenMydisInfoString("cluster", db)
			return protocol.MakeBulkReply(reply)
		case "keyspace":
			reply := GenMydisInfoString("keyspace", db)
			return protocol.MakeBulkReply(reply)
		}
	}
	return protocol.MakeArgNumErrReply("info")
}

func GenMydisInfoString(section string, db *Server) []byte {
	startUpTimeNow := getMydisRuningTime()
	switch section {
	case "server":
		str := fmt.Sprintf(
			"# Server \r\n"+
				"mydis_version:%s\r\n"+
				"mydis_mode:%s\r\n"+
				"os:%s %s\r\n"+
				"arch_bits:%d\r\n"+
				"go_version:%s\r\n"+
				"process_id:%d\r\n"+
				"run_id:%s\r\n"+
				"tcp_port:%d\r\n"+
				"uptime_in_seconds:%d\r\n"+
				"uptime_in_days:%d\r\n"+
				"config_file:%s\r\n",
			mydisVersion,
			getMydisRunningMode(),
			runtime.GOOS,
			runtime.GOARCH,
			32<<(^uint(0)>>63),
			runtime.Version(),
			os.Getegid(),
			config.Properties.RunID,
			config.Properties.Port,
			startUpTimeNow,
			startUpTimeNow/time.Duration(3600*24),
			config.Properties.CfgPath,
		)
		return []byte(str)
	case "client":
		str := fmt.Sprintf("# Clients\r\n")
		return []byte(str)
	case "cluster":
		if getMydisRunningMode() == config.ClusterMode {
			str := fmt.Sprintf("# Cluster\r\n"+
				"cluster_enabled:%s\r\n",
				"1",
			)
			return []byte(str)
		} else {
			str := fmt.Sprintf("# Cluster\r\n"+
				"cluster_enabled:%s\r\n",
				"0",
			)
			return []byte(str)
		}
	case "keyspace":
		dbCount := config.Properties.Databases
		var serv []byte
		for i := 0; i < dbCount; i++ {
			keys, expiresKeys := db.GetDBSize(i)
			if keys != 0 {
				ttlSampleAverage := db.GetAvgTTL(i, 20)
				serv = append(serv, getDbSize(i, keys, expiresKeys, ttlSampleAverage)...)
			}
		}
		prefix := []byte("# Keyspace\r\n")
		keyspaceInfo := append(prefix, serv...)
		return keyspaceInfo
	}
	return []byte("")
}

// 获取 Mydis 运行模式
func getMydisRunningMode() string {
	if config.Properties.ClusterEnable {
		return config.ClusterMode
	} else {
		return config.StandaloneMode
	}
}

// 获取 Mydis 持续运行时间
func getMydisRuningTime() time.Duration {
	return time.Since(config.EachTimeServerInfo.StartUpTime) / time.Second
}

// 获取数据库中各种键值对的数量
func getDbSize(dbIndex, keys, expiresKeys int, ttl int64) []byte {
	size := fmt.Sprintf("db%d:keys=%d,expires=%d,avg_ttl=%d\r\n",
		dbIndex, keys, expiresKeys, ttl)
	return []byte(size)
}
