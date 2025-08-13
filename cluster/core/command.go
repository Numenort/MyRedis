package core

import (
	"fmt"
	"myredis/config"
	"myredis/database"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/protocol"
	"runtime/debug"
	"strings"
)

type CmdFunc func(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply

var commands = make(map[string]CmdFunc)

// 向集群注册一个新的命令处理器
func RegisterCmd(names string, cmd CmdFunc) {
	name := strings.ToLower(names)
	commands[name] = cmd
}

// 集群模式下执行命令
func (cluster *Cluster) Exec(c myredis.Connection, cmdLine [][]byte) (result myredis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return database.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	cmdFunc, ok := commands[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode.")
	}
	return cmdFunc(cluster, c, cmdLine)
}

// 检查客户端是否已完成身份验证
func isAuthenticated(c myredis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// 默认处理函数：如果目标节点是本机，则直接在本地数据库执行；远程节点，通过 Relay 转发
// 参数：
//   - cluster: 集群实例
//   - c: 客户端连接
//   - args: 命令参数（不含命令名，但此处 args[1] 是 key）
func DefaultFunc(cluster *Cluster, c myredis.Connection, args [][]byte) myredis.Reply {
	key := string(args[1])
	// 对应的哈希槽
	slotID := cluster.GetSlot(key)
	// 查询负责该槽的节点 ID
	peerID := cluster.PickNode(slotID)
	// 如果是自身节点
	if peerID == cluster.SelfID() {
		return cluster.db.Exec(c, args)
	}
	// 让其他节点执行该命令
	return cluster.Relay(peerID, c, args)
}

func RegisterDefaultCmd(name string) {
	RegisterCmd(name, DefaultFunc)
}
