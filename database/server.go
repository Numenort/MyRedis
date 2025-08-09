package database

import (
	"fmt"
	"myredis/aof"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/protocol"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

var mydisVersion string = "1.0.0"

type Server struct {
	dbSet     []*atomic.Value
	persister *aof.Persister

	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

// func NewStandaloneServer() *Server {
// 	server := &Server{}
// 	if config.Properties.Databases == 0 {
// 		config.Properties.Databases = 16
// 	}
// 	// 创建 myredis 所需的临时目录
// 	err := os.MkdirAll(config.GetTmpDir(), os.ModePerm)
// 	if err != nil {
// 		panic(fmt.Sprintf("create temp dir failed: %v", err))
// 	}

// 	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
// 	for i := range server.dbSet {
// 		singleDB := makeDB()
// 		singleDB.index = i
// 		holder := &atomic.Value{}
// 		holder.Store(singleDB)
// 		server.dbSet[i] = holder
// 	}

// 	// 如果开启了 AOF 持久化
// 	vaildAof := false
// 	if config.Properties.AppendOnly {
// 		vaildAof = fileExists(config.Properties.AppendFilename)

// 	}
// }

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err != nil && !info.IsDir()
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

func (server *Server) Exec(c myredis.Connection, cmdLine [][]byte) (result myredis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
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
	return nil
}

func (server *Server) AfterClientClose(c myredis.Connection) {

}

func (server *Server) Close() {

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
