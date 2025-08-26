// 该包主要包含以下功能：
//   - ExistsIn: 返回请求中所有已存在的键名。
//   - DumpKey: 将指定 key 的值及其 TTL 序列化为可传输的命令形式。
//   - RenameFrom / RenameTo / RenameNxTo: 配合实现跨节点的 key 重命名（迁移）操作，支持原子性与存在性检查。
//   - CopyFrom / CopyTo: 支持跨节点的 key 复制（非移动），用于数据副本创建。

package database

import (
	"myredis/aof"
	"myredis/interface/myredis"
	"myredis/myredis/parser"
	"myredis/protocol"
)

// 返回参数中所有已存在的键名
// 命令格式：ExistsIn key1 key2 key3 ...
func execExistIn(db *DB, args [][]byte) myredis.Reply {
	var result [][]byte
	// 解析键
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result = append(result, []byte(key))
		}
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

// 返回指定 key 的序列化表示
// 命令格式：dumpKey key
func execDumpKey(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return protocol.MakeEmptyMultiBulkReply()
	}
	dumpCmd := aof.EntityToCmd(key, entity)
	ttlCmd := toTTLCmd(db, key)
	resp := protocol.MakeMultiBulkReply(
		[][]byte{
			dumpCmd.ToBytes(),
			ttlCmd.ToBytes(),
		})
	return resp
}

// 删除指定 key，等价于 DEL 命令
// 作为集群 RENAME 操作的源端清理动作
// 参数格式：renameFrom key
func execRenameFrom(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	db.Remove(key)
	return protocol.MakeOkReply()
}

// 使用由 execDumpKey 生成的序列化命令，在目标节点上重建一个 key
// 参数格式：renameTo key dumpCmd ttlCmd
func execRenameTo(db *DB, args [][]byte) myredis.Reply {
	key := args[0]
	// 解析 dumpCmd（原始 value 的重建命令）
	dumpRawCmd, err := parser.ParseOne(args[1])
	if err != nil {
		return protocol.MakeErrReply("illegal dump cmd: " + err.Error())
	}
	// 解析为 MultiBulkReply 格式
	dumpCmd, ok := dumpRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("dump cmd is not multi bulk reply")
	}
	// 设置 key 为新目标的 key
	dumpCmd.Args[1] = key

	// 对 TTL 命令执行相应的操作
	ttlRawCmd, err := parser.ParseOne(args[2])
	if err != nil {
		return protocol.MakeErrReply("illegal ttl cmd: " + err.Error())
	}
	ttlCmd, ok := ttlRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("ttl cmd is not multi bulk reply")
	}
	ttlCmd.Args[1] = key

	// 删除存在的同名 key
	db.Remove(string(key))
	// 恢复 value
	dumpResult := db.execWithLock(dumpCmd.Args)
	if protocol.IsErrorReply(dumpResult) {
		return dumpResult
	}
	// 恢复 TTL
	ttlResult := db.execWithLock(ttlCmd.Args)
	if protocol.IsErrorReply(ttlResult) {
		return ttlResult
	}
	return protocol.MakeOkReply()
}

// 用于 RenameNx 操作（目标 key 不存在时才迁移）
// 存在性检查由 cluster.prepareRenameNxTo 提前完成
func execRenameNxTo(db *DB, args [][]byte) myredis.Reply {
	return execRenameTo(db, args)
}

// 开始复制流程
func execCopyFrom(db *DB, args [][]byte) myredis.Reply {
	return protocol.MakeOkReply()
}

// 使用 dump 数据在目标节点创建副本
// 参数命令：execCopyTo key dumpCmd ttlCmd
func execCopyTo(db *DB, args [][]byte) myredis.Reply {
	key := args[0]
	// 解析 dumpCmd（原始 value 的重建命令）
	dumpRawCmd, err := parser.ParseOne(args[1])
	if err != nil {
		return protocol.MakeErrReply("illegal dump cmd: " + err.Error())
	}
	dumpCmd, ok := dumpRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("dump cmd is not multi bulk reply")
	}
	// 设置 cmd 的 key
	dumpCmd.Args[1] = key

	// 解析 TTLCmd
	ttlRawCmd, err := parser.ParseOne(args[2])
	if err != nil {
		return protocol.MakeErrReply("illegal ttl cmd: " + err.Error())
	}
	ttlCmd, ok := ttlRawCmd.(*protocol.MultiBulkReply)
	if !ok {
		return protocol.MakeErrReply("ttl cmd is not multi bulk reply")
	}
	ttlCmd.Args[1] = key

	// 删除存在的同名 key (覆盖)
	db.Remove(string(key))
	// 恢复原始值
	dumpResult := db.execWithLock(dumpCmd.Args)
	if protocol.IsErrorReply(dumpResult) {
		return dumpResult
	}
	ttlResult := db.execWithLock(ttlCmd.Args)
	if protocol.IsErrorReply(ttlResult) {
		return ttlResult
	}
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("ExistIn", execExistIn, readAllKeys, nil, -1, flagReadOnly)
	registerCommand("DumpKey", execDumpKey, writeAllKeys, undoDel, 2, flagReadOnly)
	registerCommand("RenameFrom", execRenameFrom, readFirstKey, nil, 2, flagWrite)
	registerCommand("RenameTo", execRenameTo, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	registerCommand("RenameNxTo", execRenameNxTo, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	registerCommand("CopyFrom", execCopyFrom, readFirstKey, nil, 2, flagReadOnly)
	registerCommand("CopyTo", execCopyTo, writeFirstKey, rollbackFirstKey, 5, flagWrite)
}
