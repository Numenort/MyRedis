package database

import (
	"myredis/interface/myredis"
	"myredis/protocol"
	"strings"
)

const (
	// 写操作命令，会修改数据
	redisFlagWrite = "write"
	// 只读命令，不会修改数据
	redisFlagReadonly = "readonly"
	// 命令结果具有随机性
	redisFlagRandom = "random"
	// 管理员命令
	redisFlagAdmin = "admin"
	// 订阅/发布相关命令
	redisFlagPubsub = "pubsub"
	// 不能在脚本中执行的命令
	redisFlagNoScript = "noscript"
	// 在数据加载期间可以执行的命令
	redisFlagLoading = "loading"
	// 在从节点上可以执行的命令
	redisFlagStale = "stale"
	// 内存不足时拒绝执行该命令
	redisFlagDenyOOM = "denyoom"
	// 不被 MONITOR 命令监控的命令
	redisFlagSkipMonitor = "skip_monitor"
	// 集群模式下的 ASKING 状态相关命令
	redisFlagAsking = "asking"
	// 快速执行的命令
	redisFlagFast = "fast"
	// 脚本执行环境中需要确定性排序的命令
	redisFlagSortForScript = "sortforscript"
	// 键的位置不固定，需通过规则动态计算的命令
	redisFlagMovableKeys = "movablekeys"
	// 订阅相关命令
	redisFlagPubSub = "pubsub"
)

func execCommand(args [][]byte) myredis.Reply {
	if len(args) == 0 {
		return getAllMydisCommandReply(args)
	}
	subCommand := strings.ToLower(string(args[0]))
	if subCommand == "info" {
		return getCommands(args[1:])
	} else if subCommand == "count" {
		return protocol.MakeIntReply(int64(len(cmdTable)))
	} else if subCommand == "getkeys" {
		if len(args) < 2 {
			return protocol.MakeErrReply("wrong number of arguments for 'commnad|" + subCommand + "'")
		}
		return getKeys(args[1:])
	} else {
		return protocol.MakeErrReply("Unknown subcommand '" + subCommand + "'")
	}
}

// 返回所有已注册 Redis 命令的详细描述信息
func getAllMydisCommandReply(args [][]byte) myredis.Reply {
	replies := make([]myredis.Reply, len(args))
	for _, cmd := range cmdTable {
		replies = append(replies, cmd.toDescReply())
	}
	return protocol.MakeMultiRawReply(replies)
}

// 返回指定命令的详细描述信息
func getCommands(args [][]byte) myredis.Reply {
	replies := make([]myredis.Reply, len(args))
	for i, arg := range args {
		cmd, ok := cmdTable[string(arg)]
		if ok {
			replies[i] = cmd.toDescReply()
		} else {
			replies[i] = protocol.MakeNullBulkReply()
		}
	}
	return protocol.MakeMultiRawReply(replies)
}

// 得到命令操作的键名列表
func getKeys(args [][]byte) myredis.Reply {
	cmdName := string(args[0])
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("Invaild specified command")
	}
	// 检查命令参数长度
	if !validateArity(cmd.arity, args[1:]) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	if cmd.prepare == nil {
		return protocol.MakeErrReply("The command has no key arguments")
	}
	writeKeys, readKeys := cmd.prepare(args[1:])
	keys := append(writeKeys, readKeys...)
	resp := make([][]byte, len(keys))
	for i, key := range keys {
		resp[i] = []byte(key)
	}
	return protocol.MakeMultiBulkReply(resp)
}

// 在服务器层面（server.go）注册的命令
func init() {
	registerSpecialCommand("Command", 0, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Keys", 2, 0).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 0, 0, 0)
	registerSpecialCommand("Auth", 2, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagSkipMonitor, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Info", -1, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("SlaveOf", 3, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Subscribe", -2, 0).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Publish", 3, 0).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("FlushAll", -1, 0).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerSpecialCommand("FlushDB", -1, 0).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerSpecialCommand("Save", -1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerSpecialCommand("BgSave", 1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerSpecialCommand("Select", 2, 0).
		attachCommandExtra([]string{redisFlagLoading, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("ReplConf", -1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	//attachCommandExtra("ReplConf", 3, []string{redisFlagReadonly, redisFlagAdmin, redisFlagNoScript}, 0, 0, 0, nil)

	// transaction command
	registerSpecialCommand("Multi", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Discard", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Exec", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagSkipMonitor}, 0, 0, 0)
	registerSpecialCommand("Watch", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 1, -1, 1)
}
