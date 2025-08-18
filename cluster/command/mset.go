package command

import (
	"myredis/cluster/core"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
	"strings"
)

func execMSetInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("mset")
	}
	cmdLine[0] = []byte("mset")
	return cluster.LocalExec(c, cmdLine)
}

func execMGetInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("mget")
	}
	cmdLine[0] = []byte("mget")
	return cluster.LocalExec(c, cmdLine)
}

func execMSetNxInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("msetnx")
	}
	cmdLine[0] = []byte("msetnx")
	return cluster.LocalExec(c, cmdLine)
}

func execMSet(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("mset")
	}
	var keys []string
	keyValues := make(map[string][]byte)
	for i := 1; i < len(cmdLine); i += 2 {
		key := string(cmdLine[i])
		value := cmdLine[i+1]
		keyValues[key] = value
		keys = append(keys, key)
	}

	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		// 只有一个节点
		for node := range routeMap {
			cmdLine[0] = []byte("mset_")
			return cluster.Relay(node, c, cmdLine)
		}
	}
	// 涉及多个节点
	cmdLineMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		nodeCmdLine := utils.ToCmdLine("mset")
		for _, key := range keys {
			val := keyValues[key]
			nodeCmdLine = append(nodeCmdLine, []byte(key), val)
		}
		cmdLineMap[node] = nodeCmdLine
	}
	tx := &TccTx{
		rawCmdLine: cmdLine,
		routeMap:   routeMap,
		cmdLines:   cmdLineMap,
	}
	_, err := doTcc(cluster, c, tx)
	if err != nil {
		return err
	}
	return protocol.MakeOkReply()
}

func execMGet(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("mget")
	}
	keys := make([]string, len(cmdLine)-1)
	for i := 1; i < len(cmdLine); i++ {
		keys = append(keys, string(cmdLine[i]))
	}
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		// 涉及单个节点
		for node := range routeMap {
			cmdLine[0] = []byte("mget_")
			cluster.Relay(node, c, cmdLine)
		}
	}

	// node -> cmdline
	cmdLineMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		cmdLineMap[node] = utils.ToCmdLine2("mget", keys...)
	}
	// 构建事务
	tx := &TccTx{
		rawCmdLine: cmdLine,
		routeMap:   routeMap,
		cmdLines:   cmdLineMap,
	}
	// 获取事务结果
	results, err := doTcc(cluster, c, tx)
	if err != nil {
		return err
	}
	// 获取 key 对应的查询结果，可能位于多个节点
	keyValues := make(map[string][]byte, len(cmdLine)-1)
	for node, res := range results {
		nodeCmdLine := cmdLineMap[node]
		result := res.(*protocol.MultiBulkReply)
		if len(result.Args) != len(nodeCmdLine)-1 {
			return protocol.MakeErrReply("wrong response from node " + node)
		}
		// 获得对应 node 的 key 查询结果，加入总查询结果中
		for i := 1; i < len(nodeCmdLine); i++ {
			key := string(nodeCmdLine[i])
			value := result.Args[i-1]
			keyValues[key] = value
		}
	}
	// 构造回复
	result := make([][]byte, 0, len(cmdLine)-1)
	for i := 1; i < len(cmdLine); i++ {
		value := keyValues[string(cmdLine[i])]
		result = append(result, value)
	}
	return protocol.MakeMultiBulkReply(result)
}

var someKeysExistsErr = "Some Keys Exists"

func execMSetNx(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 || len(cmdLine)%2 != 1 {
		return protocol.MakeArgNumErrReply("mset")
	}
	var keys []string
	// 存储 key <-> time
	keyValues := make(map[string][]byte)
	for i := 1; i < len(cmdLine); i += 2 {
		key := string(cmdLine[i])
		value := cmdLine[i+1]
		keyValues[key] = value
		keys = append(keys, key)
	}
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		for node := range routeMap {
			cmdLine[0] = []byte("msetnx_")
			return cluster.Relay(node, c, cmdLine)
		}
	}

	// 构造节点->命令映射表
	cmdLineMap := make(map[string]CmdLine)
	for node, keys := range routeMap {
		nodeCmdLine := utils.ToCmdLine("msetnx")
		for _, key := range keys {
			val := keyValues[key]
			nodeCmdLine = append(nodeCmdLine, []byte(key), val)
		}
		cmdLineMap[node] = nodeCmdLine
	}

	tx := &TccTx{
		rawCmdLine: cmdLine,
		routeMap:   routeMap,
		cmdLines:   cmdLineMap,
	}
	_, err := doTcc(cluster, c, tx)
	if err != nil {
		if strings.Contains(err.Error(), someKeysExistsErr) {
			return protocol.MakeIntReply(0)
		}
		return err
	}
	return protocol.MakeIntReply(1)
}

// 执行前预检查 keys 是否存在
func msetNxPrecheck(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	var keys []string
	for i := 1; i < len(cmdLine); i += 2 {
		keys = append(keys, string(cmdLine[i]))
	}
	exists := cluster.LocalExists(keys)
	if len(exists) > 0 {
		return protocol.MakeErrReply(someKeysExistsErr)
	}
	return protocol.MakeOkReply()
}

func init() {
	core.RegisterCmd("mset_", execMSetInLocal)
	core.RegisterCmd("mset", execMSet)
	core.RegisterCmd("mget_", execMGetInLocal)
	core.RegisterCmd("mget", execMGet)
	core.RegisterCmd("msetnx_", execMSetNxInLocal)
	core.RegisterCmd("msetnx", execMSetNx)
	core.RegisterPrepareFunc("msetnx", msetNxPrecheck)
}
