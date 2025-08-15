// 用于在 Mydis 中支持跨节点或复杂操作的原子性事务控制

package core

import (
	"myredis/database"
	"myredis/interface/myredis"
	"myredis/lib/timewheel"
	"myredis/protocol"
	"strings"
	"sync"
	"time"
)

const transactionTTL = time.Minute

var prepareFuncs = make(map[string]CmdFunc)

func RegisterPrepareFunc(name string, fn CmdFunc) {
	name = strings.ToLower(name)
	prepareFuncs[name] = fn
}

type TransactionManager struct {
	txs map[string]*TCC // 事务映射表
	mu  sync.RWMutex
}

/* 分布式事务实例，包含事务的上下文信息 */
type TCC struct {
	realCmdLine CmdLine // 实际要执行的命令行
	hasLock     bool    // 标记当前事务是否已持有相关键的锁
	undoLogs    []CmdLine
	writeKeys   []string
	readKeys    []string
}

func newTransactionManager() *TransactionManager {
	return &TransactionManager{
		txs: make(map[string]*TCC),
	}
}

// 处理 `prepare` 命令，启动一个 TCC 事务。
// 命令格式：prepare txid command [args...]
// 返回值：
//   - redis.Reply: OK 表示准备成功；错误回复表示失败，需触发 rollback。
func execPrepare(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("prepare")
	}
	txID := string(cmdLine[1])
	commandLine := cmdLine[2:]

	// 创建事务，是否已经存在
	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txID]
	if tx != nil {
		cluster.transactions.mu.Unlock()
		return protocol.MakeErrReply("transaction exists")
	}
	// 创建新的事务，写入事务映射表
	tx = &TCC{}
	cluster.transactions.txs[txID] = tx
	cluster.transactions.mu.Unlock()

	// 1.分析命令的读写键
	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(commandLine)
	// 2.进行加锁
	cluster.db.RWLocks(0, tx.writeKeys, tx.readKeys)
	tx.hasLock = true
	// 3.生成回滚日志
	tx.undoLogs = cluster.db.GetUndoLogs(0, commandLine)
	// 4.保存命令，等待执行
	tx.realCmdLine = commandLine
	// 5.执行自定义的准备函数
	prapareFunc := prepareFuncs[strings.ToLower(string(commandLine[0]))]
	var result myredis.Reply
	if prapareFunc != nil {
		result = prapareFunc(cluster, c, commandLine)
	} else {
		result = protocol.MakeOkReply()
	}
	// 准备函数调用失败，释放锁
	if protocol.IsErrorReply(result) {
		cluster.db.RWUnLocks(0, tx.writeKeys, tx.readKeys)
		tx.hasLock = false
	}
	return result
}

// 处理 `commit` 命令，确认并执行一个已 prepare 的事务
// 命令格式：commit txid
func execCommit(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("commit")
	}
	txID := string(cmdLine[1])

	// 获取具体事务
	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txID]
	cluster.transactions.mu.Unlock()
	if tx == nil {
		return protocol.MakeErrReply("transcaion not found")
	}

	// 执行这个命令
	resp := cluster.db.ExecWithLock(c, tx.realCmdLine)
	// 事务失败，等待回滚
	if protocol.IsErrorReply(resp) {
		return resp
	}
	cluster.db.RWUnLocks(0, tx.writeKeys, tx.readKeys)
	tx.hasLock = false
	// 提交成功：延迟删除事务
	timewheel.At(time.Now().Add(transactionTTL), txID, func() {
		cluster.transactions.mu.Lock()
		delete(cluster.transactions.txs, txID)
		cluster.transactions.mu.Unlock()
	})

	return resp
}

func execRollback(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("rollback")
	}
	txID := string(cmdLine[1])
	// 安全获取事务
	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txID]
	cluster.transactions.mu.Unlock()
	if tx == nil {
		return protocol.MakeErrReply("transaction not founc")
	}
	// 确保对应的键加锁
	if !tx.hasLock {
		cluster.db.RWLocks(0, tx.writeKeys, tx.readKeys)
		tx.hasLock = true
	}
	// 逆序执行回滚命令
	for i := len(tx.undoLogs) - 1; i >= 0; i-- {
		cmdLine := tx.undoLogs[i]
		cluster.db.ExecWithLock(c, cmdLine)
	}
	cluster.db.RWUnLocks(0, tx.writeKeys, tx.readKeys)
	// 移除对应的事务
	cluster.transactions.mu.Lock()
	delete(cluster.transactions.txs, txID)
	cluster.transactions.mu.Unlock()

	return protocol.MakeOkReply()
}

func init() {
	RegisterCmd("prepare", execPrepare)
	RegisterCmd("commit", execCommit)
	RegisterCmd("rollback", execRollback)
}
