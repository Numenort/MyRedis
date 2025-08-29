package database

import (
	"fmt"
	"myredis/aof"
	"myredis/config"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/protocol"
	"myredis/pubsub"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var mydisVersion string = "1.0.0"

type Server struct {
	dbSet     []*atomic.Value // 数据库实例
	persister *aof.Persister  // aof 持久化
	hub       *pubsub.Hub     // 处理订阅事务

	role         int32         // 标识当前服务器的角色
	slaveStatus  *slaveStatus  // 作为从节点时的状态
	masterStatus *masterStatus // 作为主节点时的状态

	insertCallback database.KeyEventCallback // 键被添加时的回调函数
	deleteCallback database.KeyEventCallback // 键被删除时的回调函数
}

// 创建一个独立的 mydis 服务器实例
func NewStandaloneServer() *Server {
	server := &Server{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// 创建所需临时目录 (RDB)
	err := os.MkdirAll(config.GetTmpDir(), os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create temp dir failed: %v", err))
	}

	// 创建数据库实例
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}

	// 创建订阅管理中心
	server.hub = pubsub.MakeHub()

	// 初始化 AOF 持久化
	validAof := false
	if config.Properties.AppendOnly {
		validAof = fileExists(config.Properties.AppendFilename)
		// 创建 AOF 管理器
		aofHandler, err := NewPersister(
			server,
			config.Properties.AppendFilename,
			true,
			config.Properties.AppendFsync,
		)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofHandler)
	}

	// 如果配置了 RDB 文件且没有有效的 AOF 文件，则尝试加载 RDB 文件
	if config.Properties.RDBFilename != "" && !validAof {
		err := server.loadRdbFile()
		if err != nil {
			logger.Error(err)
		}
	}

	// 初始化主从复制状态
	server.slaveStatus = initReplSlaveStatus()
	server.initMasterStatus()
	// 启动主从复制定时任务
	server.startReplCron()
	server.role = masterRole
	return server
}

// 启动定时任务，周期性地执行主从复制相关维护任务
func (server *Server) startReplCron() {
	go func(mdb *Server) {
		ticker := time.Tick(10 * time.Second)
		for range ticker {
			mdb.slaveCron()
			mdb.masterCron()
		}
	}(server)
}

/*---------- 数据库相关操作 ----------*/

// selectDB 根据数据库索引安全地获取对应的数据库实例。
// 如果索引超出范围，返回 nil 和一个错误回复。
func (server *Server) selectDB(index int) (*DB, *protocol.StandardErrReply) {
	if index > len(server.dbSet) || index < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[index].Load().(*DB), nil
}

// mustSelectDB 据数据库索引安全地获取对应的数据库实例。
// 假设传入的索引一定是合法的
func (server *Server) mustSelectDB(index int) *DB {
	selectDB, err := server.selectDB(index)
	if err != nil {
		panic(err)
	}
	return selectDB
}

// 利用新数据库的内容替换掉旧数据库
func (server *Server) loadDB(dbIndex int, newDB *DB) myredis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := server.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof
	server.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

// 执行 FLUSHDB 命令，先将命令写入 AOF 文件，再清空数据库
func (server *Server) execFlushDB(dbIndex int) myredis.Reply {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, utils.ToCmdLine("FlushDB"))
	}
	return server.flushDB(dbIndex)
}

// 清空指定索引的数据库
func (server *Server) flushDB(dbIndex int) myredis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	newDB := makeDB()
	server.loadDB(dbIndex, newDB)
	return protocol.MakeOkReply()
}

// 清空所有数据库
func (server *Server) flushAll() myredis.Reply {
	for i := range server.dbSet {
		server.flushDB(i)
	}
	// 记录到 AOF 文件中
	if server.persister != nil {
		server.persister.SaveCmdLine(0, utils.ToCmdLine("FlushAll"))
	}
	return protocol.MakeOkReply()
}

/* ---------- 实现数据库接口 ---------- */

// 解析命令并将其路由到正确的处理程序
func (server *Server) Exec(c myredis.Connection, cmdLine [][]byte) (result myredis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// 无需认证的命令
	if cmdName == "ping" {
		return Ping(c, cmdLine[1:])
	}
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	if cmdName == "info" {
		return Info(server, cmdLine[1:])
	}
	if cmdName == "dbsize" {
		return Dbsize(c, server)
	}
	if cmdName == "command" {
		return execCommand(cmdLine[1:])
	}
	if cmdName == "slaveof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot use slave of database within multi")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("SLAVEOF")
		}
		return server.execSlaveOf(c, cmdLine[1:])
	}
	// 从节点只允许执行读命令
	role := atomic.LoadInt32(&server.role)
	if role == slaveRole && !c.IsMaster() {
		if !isReadOnlyCommand(cmdName) {
			return protocol.MakeErrReply("READONLY You can't write against a read only slave.")
		}
	}

	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(server.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		return BGRewriteAOF(server, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(server, cmdLine[1:])
	} else if cmdName == "flushall" {
		return server.flushAll()
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used int MULTI")
		}
		return server.execFlushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return SaveRDB(server, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(server, cmdLine[1:])
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, server, cmdLine[1:])
	} else if cmdName == "copy" {
		if len(cmdLine) < 3 {
			return protocol.MakeArgNumErrReply("copy")
		}
		return execCopy(server, c, cmdLine[1:])
	} else if cmdName == "replconf" {
		return server.execReplConf(c, cmdLine[1:])
	} else if cmdName == "psync" {
		return server.execPSync(c, cmdLine[1:])
	}

	// 对于普通命令，分发到客户端当前选择的数据库进行处理
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(c, cmdLine)
}

func (server *Server) AfterClientClose(c myredis.Connection) {
	// 从服务端的订阅中心移除该连接的所有订阅
	pubsub.UnSubscribeAll(server.hub, c)
}

func (server *Server) Close() {
	// 停止从节点功能
	server.slaveStatus.close()
	// 停止持久化功能
	if server.persister != nil {
		server.persister.Close()
	}
	// 停止主节点功能
	server.stopMaster()
}

func (server *Server) ExecMulti(conn myredis.Connection, watching map[string]uint32, cmdLines []CmdLine) myredis.Reply {
	// 连接实例中获取数据库 id
	selectDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectDB.ExecMulti(conn, watching, cmdLines)
}

func (server *Server) ExecWithLock(conn myredis.Connection, cmdLine [][]byte) myredis.Reply {
	selectDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectDB.execWithLock(cmdLine)
}

func (server *Server) ForEach(dbIndex int, callback func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	server.mustSelectDB(dbIndex).ForEach(callback)
}

func (server *Server) GetDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

func (server *Server) GetEntity(dbIndex int, key string) (*database.DataEntity, bool) {
	db := server.mustSelectDB(dbIndex)
	return db.GetEntity(key)
}

func (server *Server) GetExpiration(dbIndex int, key string) *time.Time {
	db := server.mustSelectDB(dbIndex)
	rawTTL, ok := db.ttlMap.Get(key)
	if !ok {
		return nil
	}
	ttlTime, _ := rawTTL.(time.Time)
	return &ttlTime
}

func (server *Server) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	db := server.mustSelectDB(dbIndex)
	db.RWLocks(writeKeys, readKeys)
}

func (server *Server) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	db := server.mustSelectDB(dbIndex)
	db.RWUnLocks(writeKeys, readKeys)
}

func (server *Server) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return server.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

// 键被删除时的回调函数
func (server *Server) SetKeyDeletedCallback(callback database.KeyEventCallback) {
	server.deleteCallback = callback
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.deleteCallback = callback
	}
}

// 新键被插入时的回调函数
func (server *Server) SetKeyInsertedCallback(callback database.KeyEventCallback) {
	server.insertCallback = callback
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.insertCallback = callback
	}
}

/*---------- 一些其他函数 ----------*/

func execSelect(c myredis.Connection, mdb *Server, args [][]byte) myredis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invaild DB index")
	}
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

// 估算并返回数据库中键的平均 TTL
func (server *Server) GetAvgTTL(dbIndex, randomKeyCount int) int64 {
	var ttlCount int64
	db := server.mustSelectDB(dbIndex)
	keys := db.data.RandomKeys(randomKeyCount)
	for _, key := range keys {
		t := time.Now()
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			continue
		}
		expireTime, _ := rawExpireTime.(time.Time)
		subTime := expireTime.Sub(t).Microseconds()
		if subTime > 0 {
			ttlCount += subTime
		}
	}
	return ttlCount / int64(len(keys))
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

/* ---------- 持久化 ----------*/

// 同步执行 AOF 文件重写任务
func RewriteAOF(db *Server, args [][]byte) myredis.Reply {
	err := db.persister.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// 后台异步执行 AOF 文件重写任务
func BGRewriteAOF(db *Server, args [][]byte) myredis.Reply {
	go db.persister.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// 同步地生成 RDB 快照文件
func SaveRDB(db *Server, args [][]byte) myredis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	// 生成 RDB 文件
	err := db.persister.GenerateRDB(rdbFilename)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// 后台异步地生成 RDB 快照文件
func BGSaveRDB(db *Server, args [][]byte) myredis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		rdbFilename := config.Properties.RDBFilename
		if rdbFilename == "" {
			rdbFilename = "dump.rdb"
		}
		err := db.persister.GenerateRDB(rdbFilename)
		if err != nil {
			logger.Error(err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}
