package command

import (
	"myredis/cluster/core"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
)

// 用于在集群中将 "del_" 命令转发到底层本地数据库引擎进行实际删除操作
// 命令格式：DEL_ key1 [key2 ...]
func execDelInLoacl(cluster *core.Cluster, c myredis.Connection, cmdLine core.CmdLine) myredis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("del")
	}
	cmdLine[0] = []byte("del")
	return cluster.LocalExec(c, cmdLine)
}

// 处理集群模式下的 DEL 命令，支持删除一个或多个分布在不同节点上的键
// 命令格式：DEL key1 key2 key3
// 返回被成功删除的键的数量之和
func execDel(cluster *core.Cluster, c myredis.Connection, cmdLine core.CmdLine) myredis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("del")
	}

	var keys []string
	for i := 1; i < len(cmdLine); i++ {
		key := string(cmdLine[i])
		keys = append(keys, key)
	}

	// 获取键对应的节点 -> 键关系
	routeMap := getRouteMap(cluster, keys)
	if len(routeMap) == 1 {
		for node := range routeMap {
			cmdLine[0] = []byte("del_")
			return cluster.Relay(node, c, cmdLine)
		}
	}

	// 构造节点->命令表
	cmdLineMap := make(map[string]core.CmdLine)
	for node, keys := range routeMap {
		// 构造节点命令
		nodeCmdLine := utils.ToCmdLine("del")
		// del key1, key2, ...
		for _, key := range keys {
			nodeCmdLine = append(nodeCmdLine, []byte(key))
		}
		cmdLineMap[node] = nodeCmdLine
	}
	// 构造多节点集群事务
	tx := &TccTx{
		rawCmdLine: cmdLine,
		routeMap:   routeMap,
		cmdLines:   cmdLineMap,
	}
	// 提交事务并执行
	results, err := doTcc(cluster, c, tx)
	if err != nil {
		return err
	}
	var count int64 = 0
	for _, res := range results {
		ret, ok := res.(*protocol.IntReply)
		if ok {
			count += ret.Code
		}
	}
	return protocol.MakeIntReply(count)
}

func init() {
	core.RegisterCmd("del_", execDelInLoacl)
	core.RegisterCmd("del", execDel)
}
