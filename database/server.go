package database

import (
	"myredis/aof"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/protocol"
	"os"
	"sync/atomic"
)

var myredisVersion string = "1.0.0"

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

func (server *Server) Exec(client myredis.Connection, cmdLine [][]byte) myredis.Reply {
	return nil
}
