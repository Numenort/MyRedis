package command

import (
	"myredis/cluster/core"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
)

type CmdLine = [][]byte

// 节点 -> 节点上的键
type RouteMap map[string][]string

// 根据给定的键列表，查询每个键所属的集群节点，构建路由映射表
func getRouteMap(cluster *core.Cluster, keys []string) RouteMap {
	m := make(RouteMap)
	for _, key := range keys {
		slotID := cluster.GetSlot(key)
		node := cluster.PickNode(slotID)
		m[node] = append(m[node], key)
	}
	return m
}

// 表示一个 TCC（Try-Confirm-Cancel）分布式事务上下文；
// 用于在多个集群节点上协调执行可能跨节点的写操作
type TccTx struct {
	rawCmdLine CmdLine
	routeMap   RouteMap
	cmdLines   map[string]CmdLine // 节点->命令
}

// 执行一个 TCC 分布式事务流程；
// 实现三阶段操作：Prepare → Commit → （失败时）Rollback；
// 返回每个节点在 commit 阶段返回的结果
func doTcc(cluster *core.Cluster, c myredis.Connection, tx *TccTx) (map[string]myredis.Reply, protocol.ErrorReply) {
	txID := utils.RandString(6)

	// 发送 prepare 请求
	for node, cmdLine := range tx.cmdLines {
		// prepare txID cmdlines
		prepareCmd := utils.ToCmdLine("prepare", txID)
		prepareCmd = append(prepareCmd, cmdLine...)
		// 执行命令
		reply := cluster.Relay(node, c, prepareCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txID, tx.routeMap)
			return nil, protocol.MakeErrReply("prepare failed: " + err.Error())
		}
	}

	// 发送 commit 请求
	commitCmd := utils.ToCmdLine("commit", txID)
	result := make(map[string]myredis.Reply)
	for node := range tx.routeMap {
		reply := cluster.Relay(node, c, commitCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txID, tx.routeMap)
			return nil, protocol.MakeErrReply("commit failed: " + err.Error())
		}
		result[node] = reply
	}
	return result, nil
}

// 向所有参与节点发送 ROLLBACK 命令
func requestRollback(cluster *core.Cluster, c myredis.Connection, txID string, routeMap RouteMap) {
	rollbackCmd := utils.ToCmdLine("rollback", txID)
	for node := range routeMap {
		cluster.Relay(node, c, rollbackCmd)
	}
}
