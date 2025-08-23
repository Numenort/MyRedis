package database

import (
	"fmt"
	"myredis/aof"
	"myredis/config"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/protocol"
	"myredis/pubsub"
	"os"
	"runtime/debug"
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
	vaildAof := false
	if config.Properties.AppendOnly {
		vaildAof = fileExists(config.Properties.AppendFilename)
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
	if config.Properties.RDBFilename != "" && !vaildAof {
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
			mdb.masterCorn()
		}
	}(server)
}

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
	if dbIndex > len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := server.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof
	server.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
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
	return nil
}

func (server *Server) AfterClientClose(c myredis.Connection) {
	// 从服务端的订阅中心移除该连接的所有订阅
	pubsub.UnSubscribeAll(server.hub, c)
}

func (server *Server) Close() {
	if server.persister != nil {
		server.persister.Close()
	}
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

func (server *Server) SetKeyDeletedCallback(callback database.KeyEventCallback) {
	server.deleteCallback = callback
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.deleteCallback = callback
	}
}

func (server *Server) SetKeyInsertedCallback(callback database.KeyEventCallback) {
	server.deleteCallback = callback
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.insertCallback = callback
	}
}

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
	return err != nil && !info.IsDir()
}
