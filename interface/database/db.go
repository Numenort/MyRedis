package database

import (
	"myredis/interface/myredis"
	"time"

	"github.com/hdt3213/rdb/core"
)

type CmdLine = [][]byte

// DB 需要实现：命令的执行、处理客户端连接关闭、关闭数据库、加载 RDB 快照
type DB interface {
	Exec(client myredis.Connection, cmdLine [][]byte) myredis.Reply
	AfterClientClose(c myredis.Connection)
	Close()
	LoadRDB(dec *core.Decoder) error
}

// key 事件回调函数
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)

type DBEngine interface {
	DB
	ExecWithLock(conn myredis.Connection, cmdLine [][]byte) myredis.Reply
	ExecMulti(conn myredis.Connection, cmdLine [][]byte) myredis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
	GetEntity(dbIndex int, key string) (*DataEntity, bool)
	GetExpiration(dbIndex int, key string) *time.Time
	SetKeyInsertedCallback(cb KeyEventCallback)
	SetKeyDeletedCallback(cb KeyEventCallback)
}

type DataEntity struct {
	Data interface{}
}
