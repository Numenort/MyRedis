package command

import (
	"myredis/cluster/core"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
)

var keyExistsErr = "key exists"

func execRenameInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	cmdLine[0] = []byte("rename")
	return cluster.LocalExec(c, cmdLine)
}

func execRenameNxInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	cmdLine[0] = []byte("renamenx")
	return cluster.LocalExec(c, cmdLine)
}

// 重命名 key
// 命令格式：RENAME src target
func execRename(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 3 {
		return protocol.MakeArgNumErrReply("rename")
	}
	// 解析命令行
	src := string(cmdLine[1])
	target := string(cmdLine[2])
	srcSlot := cluster.GetSlot(src)
	srcNode := cluster.PickNode(srcSlot)
	targetSlot := cluster.GetSlot(target)
	targetNode := cluster.PickNode(targetSlot)
	// 在同一个节点下面
	if srcNode == targetNode {
		cmdLine[0] = []byte("rename")
		return cluster.Relay(srcNode, c, cmdLine)
	}
	// 参与任务的 节点->节点上的键
	routeMap := RouteMap{
		srcNode:    {src},
		targetNode: {target},
	}

	txID := utils.RandString(10)
	// Prepare 源节点（检查 key 是否存在）
	srcPrepareResp := cluster.Relay(srcNode, c, utils.ToCmdLine("Prepare", txID, "RenameFrom", src))
	if protocol.IsErrorReply(srcPrepareResp) {
		// 源节点准备失败即回滚
		requestRollback(cluster, c, txID, routeMap)
	}
	srcPrepareResult, ok := srcPrepareResp.(*protocol.MultiBulkReply)
	if !ok || len(srcPrepareResult.Args) < 2 {
		requestRollback(cluster, c, txID, map[string][]string{srcNode: {src}})
		return protocol.MakeErrReply("ERR invalid prepare response")
	}

	// Prepare 目标节点
	// Prepare txID RenameTo target ttl dumpedValue
	targetPrepareResp := cluster.Relay(targetNode, c, utils.ToCmdLine3("Prepare", []byte(txID),
		[]byte("RenameTo"), []byte(target), srcPrepareResult.Args[0], srcPrepareResult.Args[1]))
	if protocol.IsErrorReply(targetPrepareResp) {
		// 目标节点准备失败，回滚源节点
		requestRollback(cluster, c, txID, routeMap)
		return targetPrepareResp
	}

	// 两阶段都成功，执行commit
	commitCmd := utils.ToCmdLine("commit", txID)
	for node := range routeMap {
		reply := cluster.Relay(node, c, commitCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txID, routeMap)
			return protocol.MakeErrReply("commit failed: " + err.Error())
		}
	}
	return protocol.MakeOkReply()
}

func execRenameNx(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 3 {
		return protocol.MakeArgNumErrReply("rename")
	}
	// 解析命令
	src := string(cmdLine[1])
	target := string(cmdLine[2])
	srcSlot := cluster.GetSlot(src)
	srcNode := cluster.PickNode(srcSlot)
	targetSlot := cluster.GetSlot(target)
	targetNode := cluster.PickNode(targetSlot)
	if srcNode == targetNode {
		cmdLine[0] = []byte("rename")
		return cluster.Relay(srcNode, c, cmdLine)
	}
	// 构建事务
	routeMap := RouteMap{
		srcNode:    {src},
		targetNode: {target},
	}
	txID := utils.RandString(10)
	// Prepare 源节点
	srcPrepareResp := cluster.Relay(srcNode, c, utils.ToCmdLine("Prepare", txID, "RenameFrom", src))
	if protocol.IsErrorReply(srcPrepareResp) {
		// 回滚事务
		requestRollback(cluster, c, txID, map[string][]string{srcNode: {src}})
		return srcPrepareResp
	}
	srcPrepareResult, ok := srcPrepareResp.(*protocol.MultiBulkReply)
	if !ok || len(srcPrepareResult.Args) < 2 {
		requestRollback(cluster, c, txID, map[string][]string{srcNode: {src}})
		return protocol.MakeErrReply("ERR invalid prepare response")
	}
	// prepare 目标节点
	targetPrepareResp := cluster.Relay(targetNode, c, utils.ToCmdLine3("Prepare", []byte(txID),
		[]byte("RenameNxTo"), []byte(target), srcPrepareResult.Args[0], srcPrepareResult.Args[1]))
	if result, ok := targetPrepareResp.(protocol.ErrorReply); ok {
		// 回滚（包含源节点和目标节点）
		requestRollback(cluster, c, txID, routeMap)
		if result.Error() == keyExistsErr {
			return protocol.MakeIntReply(0)
		}
		return result
	}
	// 提交事务
	commitCmd := utils.ToCmdLine("commit", txID)
	for node := range routeMap {
		// 提交事务 commit 命令
		reply := cluster.Relay(node, c, commitCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txID, routeMap)
			return protocol.MakeErrReply("commit failed: " + err.Error())
		}
	}
	return protocol.MakeIntReply(1)
}

func prepareRenameFrom(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("RenameFrom")
	}
	// 检查 key 是否存在
	key := string(cmdLine[1])
	existResp := cluster.LocalExec(c, utils.ToCmdLine("Exists", key))
	if protocol.IsErrorReply(existResp) {
		return existResp
	}
	existIntResp := existResp.(*protocol.IntReply)
	if existIntResp.Code == 0 {
		return protocol.MakeErrReply("ERR no such key")
	}
	return cluster.LocalExecWithLock(c, utils.ToCmdLine2("DumpKey", key))
}

func prepareRenameNxTo(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 4 {
		return protocol.MakeArgNumErrReply("RenameNxTo")
	}
	key := string(cmdLine[1])
	// 检查 target key 是否存在
	exists := cluster.LocalExists([]string{key})
	if len(exists) > 0 {
		return protocol.MakeErrReply(keyExistsErr)
	}
	return protocol.MakeOkReply()
}

func init() {
	core.RegisterCmd("rename_", execRenameInLocal)
	core.RegisterCmd("renamenx_", execRenameNxInLocal)
	core.RegisterCmd("rename", execRename)
	core.RegisterCmd("renamenx", execRenameNx)
	core.RegisterPrepareFunc("RenameFrom", prepareRenameFrom)
	core.RegisterPrepareFunc("RenameNxTo", prepareRenameNxTo)
}
