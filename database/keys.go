package database

import (
	"container/list"
	"math"
	"myredis/datastruct/dict"
	"myredis/lib/utils"
	"myredis/myredis"
	"myredis/protocol"
	"strconv"
	"time"
)

// 执行删除键的操作，返回删除的键的个数
func execDel(db *DB, args [][]byte) myredis.Reply {
	keys := make([]string, len(args))

	for i, value := range args {
		keys[i] = string(value)
	}

	deleted := db.Removes(keys...)

	return protocol.MakeIntReply(int64(deleted))
}

// func undoDel(db *DB, args [][]byte) myredis.Reply {
// 	keys := make([]string, len(args))
// 	for i, value := range args {
// 		keys[i] = string(value)
// 	}
// 	return rollbackGivenKeys(db, keys...)
// }

// 检查是否存在对应的键，返回存在的个数
func execExists(db *DB, args [][]byte) myredis.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return protocol.MakeIntReply(result)
}

// 清空数据库
func execFlushDB(db *DB, args [][]byte) myredis.Reply {
	db.Flush()
	return &protocol.OkReply{}
}

// 获取 key 对应值的类型
func getType(db *DB, key string) string {
	entity, exists := db.GetEntity(key)
	if !exists {
		return "none"
	}
	switch entity.Data.(type) {
	case []byte:
		return "string"
	case list.List:
		return "list"
	case dict.Dict:
		return "hash"
	}
	return ""
}

// 获取 key 对应的剩余存活时间
func execTTL(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	ttl, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}

	rawExpireTime, _ := ttl.(time.Time)
	TTL := rawExpireTime.Sub(time.Now()).Seconds()
	return protocol.MakeIntReply(int64(math.Round(TTL)))
}

// 获取 key 对应的剩余存活时间 (milliseconds)
func execPTTL(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	ttl, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}

	rawExpireTime, _ := ttl.(time.Time)
	TTL := rawExpireTime.Sub(time.Now()).Milliseconds()
	return protocol.MakeIntReply(int64(math.Round(float64(TTL))))
}

func toTTLCmd(db *DB, key string) *protocol.MultiBulkReply {
	raw, exists := db.ttlMap.Get(key)
	if !exists {
		// has no TTL
		return protocol.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	expireTime, _ := raw.(time.Time)
	timestamp := strconv.FormatInt(expireTime.UnixNano()/1000/1000, 10)
	return protocol.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}

func init() {
	registerCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("PTTL", execPTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
}
