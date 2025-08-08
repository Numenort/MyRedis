package database

import (
	"fmt"
	"myredis/aof"
	"myredis/config"
	"myredis/datastruct/dict"
	List "myredis/datastruct/list"
	HashSet "myredis/datastruct/set"
	SortedSet "myredis/datastruct/sortedset"
	"myredis/interface/database"
	"os"
	"sync/atomic"

	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
)

// 加载 rdb 文件，将命令读入数据库中
func (server *Server) loadRdbFile() error {
	// 加载 RDB 文件
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		return fmt.Errorf("open rdb file failed " + err.Error())
	}
	defer func() {
		_ = rdbFile.Close()
	}()

	decoder := rdb.NewDecoder(rdbFile)
	err = server.LoadRDB(decoder)
	if err != nil {
		return fmt.Errorf("load rdb file failed " + err.Error())
	}
	return nil
}

// 解析 RDB 文件流
func (server *Server) LoadRDB(dec *core.Decoder) error {
	// 解码器解析 RDB 文件流时，每解析出一个完整的 RedisObject，
	// 调用提供的这个回调函数
	err := dec.Parse(func(object rdb.RedisObject) bool {
		db := server.mustSelectDB(object.GetDBIndex())
		var entity *database.DataEntity
		switch object.GetType() {
		case rdb.StringType:
			str := object.(*rdb.StringObject)
			entity = &database.DataEntity{
				Data: str.Value,
			}
		case rdb.ListType:
			listObj := object.(*rdb.ListObject)
			list := List.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			entity = &database.DataEntity{
				Data: list,
			}
		case rdb.HashType:
			hashObj := object.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			entity = &database.DataEntity{
				Data: hash,
			}
		case rdb.SetType:
			setObj := object.(*rdb.SetObject)
			set := HashSet.Make()
			for _, mem := range setObj.Members {
				set.Add(string(mem))
			}
			entity = &database.DataEntity{
				Data: set,
			}
		case rdb.ZSetType:
			zsetObj := object.(*rdb.ZSetObject)
			zSet := SortedSet.Make()
			for _, e := range zsetObj.Entries {
				zSet.Add(e.Member, e.Score)
			}
			entity = &database.DataEntity{
				Data: zSet,
			}
		}
		if entity != nil {
			db.PutEntity(object.GetKey(), entity)
			if object.GetExpiration() != nil {
				db.Expire(object.GetKey(), *object.GetExpiration())
			}
			db.addAof(aof.EntityToCmd(object.GetKey(), entity).Args)
		}
		return true
	})
	return err
}

// func NewPersister(db database.DBEngine, filename string, load bool, fsync string) (*aof.Persister, error) {
// 	return aof.NewPersister(
// 		db, filename, load, fsync, func() database.DBEngine {
// 			return MakeAuxiliaryServer()
// 		})
// }

// AddAof 向 AOF 持久化器添加命令行记录，允许外部组件直接向 AOF 文件写入命令
//
// 参数:
//   - dbIndex: 数据库索引，指定命令应该记录到哪个数据库
//   - cmdLine: 命令行参数，包含完整的 Redis 命令
func (server *Server) AddAof(dbIndex int, cmdLine CmdLine) {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, cmdLine)
	}
}

// bindPersister 将 AOF 持久化器绑定到服务器，建立双向连接
// 1. 将持久化器实例保存到服务器中，用于外部调用
// 2. 为每个数据库实例设置 addAof 回调函数，实现自动持久化
// 参数:
//   - aofHandler: AOF 持久化器实例，负责实际的文件写入操作
func (server *Server) bindPersister(persister *aof.Persister) {
	server.persister = persister
	for _, db := range server.dbSet {
		singleDB := db.Load().(*DB)
		singleDB.addAof = func(line CmdLine) {
			if config.Properties.AppendOnly {
				server.persister.SaveCmdLine(singleDB.index, line)
			}
		}
	}

}

func MakeAuxiliaryServer() *Server {
	simpleServer := &Server{}
	simpleServer.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range simpleServer.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeDB())
		simpleServer.dbSet[i] = holder
	}
	return simpleServer
}
