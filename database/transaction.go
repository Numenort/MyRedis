package database

import (
	"myredis/interface/myredis"
	"myredis/protocol"
	"strings"
)

// ******************** startMulti ********************
// 启动一次事务：进入事务状态后，客户端发送的命令不会立即执行，而是被放入队列中
func StartMulti(conn myredis.Connection) myredis.Reply {
	if conn.InMultiState() {
		return protocol.MakeErrReply("ERR MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return protocol.MakeOkReply()
}

// ******************** DiscardMulti ********************
// 丢弃当前事务：清空事务队列，退出事务状态
func DiscardMulti(conn myredis.Connection) myredis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR DISCARD without MULTI")
	}
	conn.ClearQueuedCmds()
	conn.SetMultiState(false)
	return protocol.MakeOkReply()
}

// ******************** execMulti ********************
// 执行事务：在客户端处于事务状态时，原子化地执行所有排队命令
func execMulti(db *DB, conn myredis.Connection) myredis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	if len(conn.GetTxErrors()) > 0 {
		return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}
	cmdLines := conn.GetQueuedCmdLine()
	return db.ExecMulti(conn, conn.GetWatching(), cmdLines)
}

// 原子性地执行一组事务命令：使用读写锁保护涉及的键，并通过乐观锁检测 WATCH 键是否被修改
func (db *DB) ExecMulti(conn myredis.Connection, watching map[string]uint32, cmdLines []CmdLine) myredis.Reply {
	// 准备读写键
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		prepare := cmd.prepare
		// 获取读写命令
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}
	// 设置监视,防止在执行前键被改变
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	readKeys = append(readKeys, watchingKeys...)
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	// 乐观锁 检查监视的键是否改变
	if isWatchingChanged(db, watching) {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// 执行命令
	results := make([]myredis.Reply, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		undoCmdLines = append(undoCmdLines, db.GetUndoLogs(cmdLine))
		result := db.execWithLock(cmdLine)
		if protocol.IsErrorReply(result) {
			aborted = true
			// 不会回滚失败的命令
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		results = append(results, result)
	}
	// 顺利执行全部命令
	if !aborted {
		db.addVersion(writeKeys...)
		return protocol.MakeMultiRawReply(results)
	}
	// 部分undo
	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			db.execWithLock(cmdLine)
		}
	}
	return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
}

// 检查被 WATCH 的键是否有版本变化
func isWatchingChanged(db *DB, watching map[string]uint32) bool {
	for key, val := range watching {
		currentVersion := db.GetVersion(key)
		if val != currentVersion {
			return true
		}
	}
	return false
}

// 获取某条命令对应的回滚命令序列
func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

// 获取对应命令涉及的读写键
func GetRelatedKeys(CmdLine [][]byte) ([]string, []string) {
	cmdName := strings.ToLower(string(CmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}
	prepare := cmd.prepare
	if prepare == nil {
		return nil, nil
	}
	return prepare(CmdLine[1:])
}

// ******************** Watch ********************
// 监视一个或多个键，用于实现事务的条件执行
func Watch(db *DB, conn myredis.Connection, args [][]byte) myredis.Reply {
	watching := conn.GetWatching()
	for _, bkey := range args {
		key := string(bkey)
		watching[key] = db.GetVersion(key)
	}
	return protocol.MakeOkReply()
}

// ******************** EnqueueCmd ********************
// 将命令加入事务队列
func EnqueueCmd(conn myredis.Connection, cmdLine [][]byte) myredis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		err := protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
		conn.AddTxError(err) //错误累积
		return err
	}
	// prepare 为 nil时，该命令不能在事务中使用
	if cmd.prepare == nil {
		err := protocol.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
		conn.AddTxError(err)
		return err
	}
	if !validateArity(cmd.arity, cmdLine) {
		err := protocol.MakeArgNumErrReply(cmdName)
		conn.AddTxError(err)
		return err
	}
	conn.EnqueueCmd(cmdLine)
	return protocol.MakeQueuedReply()
}

// 获取 key 对应的版本信息
func execGetVersion(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	version := db.GetVersion(key)
	return protocol.MakeIntReply(int64(version))
}

func init() {
	registerCommand("GetVer", execGetVersion, readAllKeys, nil, 2, flagReadOnly)
}
