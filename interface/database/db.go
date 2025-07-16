package database

import (
	"myredis/interface/redis"
	"myredis/myredis"
)

type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) myredis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}

// key 事件回调函数
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)

type DBEngine interface {
	DB
	execWithLock(conn redis.Connection, cmdLine [][]byte) myredis.Reply
	SetKeyDeletedCallback(cb KeyEventCallback)
	SetKeyInsertedCallback(cb KeyEventCallback)
}

type DataEntity struct {
	Data interface{}
}
