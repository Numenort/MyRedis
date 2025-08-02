package database

import (
	"container/list"
	"fmt"
	"math"
	"myredis/aof"
	"myredis/datastruct/dict"
	"myredis/datastruct/set"
	"myredis/datastruct/sortedset"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/lib/wildcard"
	"myredis/protocol"
	"strconv"
	"strings"
	"time"
)

// execDel: 删除指定的一个或多个键。
// 返回值: 成功删除的键的数量。
// 格式: DEL [KEY1] [KEY2] ...
func execDel(db *DB, args [][]byte) myredis.Reply {
	keys := make([]string, len(args))

	for i, value := range args {
		keys[i] = string(value)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("del", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

// undoDel: 为 DEL 命令生成回滚操作
func undoDel(db *DB, args [][]byte) []CmdLine {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return rollbackGivenKeys(db, keys...)
}

// execExists: 检查指定的一个或多个键是否存在。
// 返回值: 存在的键的数量。
// 格式: EXISTS [KEY1] [KEY2] ...
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

// 清空当前数据库中的所有键。
// 返回值: OK
func execFlushDB(db *DB, args [][]byte) myredis.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return &protocol.OkReply{}
}

// 获取指定键的值类型
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
	case *set.Set:
		return "set"
	case *sortedset.SortedSet:
		return "zset"
	}
	return ""
}

// execType: 获取指定键的值类型。
// 返回值: 类型字符串 ("string", "list", etc.) 或 "none"。
// 格式: TYPE [KEY]
func execType(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	keyType := getType(db, key)
	if len(keyType) > 0 {
		return protocol.MakeStatusReply(keyType)
	} else {
		return &protocol.UnknownErrReply{}
	}
}

func prepareRename(args [][]byte) ([]string, []string) {
	src := string(args[0])
	dest := string(args[1])
	return []string{src}, []string{dest}
}

// execRename: 将键从 [KEY] 重命名为 [NEWKEY]。如果 [NEWKEY] 已存在，会覆盖。
// 返回值: OK
// 格式: RENAME [KEY] [NEWKEY]
func execRename(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	des := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("no such key %s", src))
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(des, entity)
	db.Remove(src)
	if hasTTL {
		// 清除原有的可能的 TTL
		db.Persist(src)
		db.Persist(des)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(des, expireTime)
	}
	db.addAof(utils.ToCmdLine3("rename", args...))
	return protocol.MakeOkReply()
}

// 为 RENAME 命令生成回滚操作
func undoRename(db *DB, args [][]byte) []CmdLine {
	src := string(args[0])
	des := string(args[1])
	return rollbackGivenKeys(db, src, des)
}

// execRenameNx: 将键从 [KEY] 重命名为 [NEWKEY]，但仅在 [NEWKEY] 不存在时才执行。
// 返回值: 1 (成功), 0 (失败，[NEWKEY] 已存在)
// 格式: RENAMENX [KEY] [NEWKEY]
func execRenameNx(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	des := string(args[1])
	// 只有目标键不存在时
	_, ok := db.GetEntity(des)
	if ok {
		return protocol.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("no such key %s", src))
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(des, entity)
	db.Remove(src)
	if hasTTL {
		db.Persist(src)
		db.Persist(des)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(des, expireTime)
	}
	db.addAof(utils.ToCmdLine3("renamecx", args...))
	return protocol.MakeIntReply(1)
}

// execExpire: 为键 [KEY] 设置过期时间，单位为秒。
// 返回值: 1 (成功), 0 (键不存在)
// 格式: EXPIRE [KEY] [SECONDS]
func execExpire(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttlSec := time.Duration(ttl) * time.Second

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttlSec)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpiredCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpiredAt: 为键 [KEY] 设置一个绝对的过期时间点（Unix 时间戳，秒）。
// 返回值: 1 (成功), 0 (键不存在)
// 格式: EXPIREAT [KEY] [TIMESTAMP]
func execExpiredAt(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	// 获取过期时间
	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpiredCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execGetExpiredTime: 获取键 [KEY] 的过期时间点（Unix 时间戳，秒）。
// 返回值: 过期时间戳 (>=0), -1 (永不过期), 0 (键不存在)
// 格式: EXPIRETIME [KEY]
func execGetExpiredTime(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	TTL, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawTTL, _ := TTL.(time.Time)
	expireTime := rawTTL.Unix()
	return protocol.MakeIntReply(expireTime)
}

// execPExpire: 为键 [KEY] 设置过期时间，单位为毫秒。
// 返回值: 1 (成功), 0 (键不存在)
// 格式: PEXPIRE [KEY] [MILLISECONDS]
func execPExpire(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ttlMilSec := time.Duration(ttl) * time.Millisecond

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now().Add(ttlMilSec)
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpiredCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execPExpiredAt: 为键 [KEY] 设置一个绝对的过期时间点（Unix 时间戳，毫秒）。
// 返回值: 1 (成功), 0 (键不存在)
// 格式: PEXPIREAT [KEY] [MILLISECONDS-TIMESTAMP]
func execPExpiredAt(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	// 获取过期时间
	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(0, raw*int64(time.Millisecond))

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpiredCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execGetPExpiredTime: 获取键 [KEY] 的过期时间点（Unix 时间戳，毫秒）。
// 返回值: 过期时间戳 (>=0), -1 (永不过期), 0 (键不存在)
// 格式: PEXPIRETIME [KEY]
func execGetPExpiredTime(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	TTL, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawTTL, _ := TTL.(time.Time)
	expireTime := rawTTL.UnixMilli()
	return protocol.MakeIntReply(expireTime)
}

// 为 EXPIRE/PERSIST 等命令生成回滚操作。
func undoExpire(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return []CmdLine{
		toTTLCmd(db, key).Args,
	}
}

// toTTLCmd: 为指定键生成重建其 TTL 的命令。
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

// execTTL: 查询键 [KEY] 的剩余存活时间，单位为秒。
// 返回值: >=0 (剩余秒数), -1 (永不过期), -2 (键不存在)
// 格式: TTL [KEY]
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

// execPTTL: 查询键 [KEY] 的剩余存活时间，单位为毫秒。
// 返回值: >=0 (剩余毫秒数), -1 (永不过期), -2 (键不存在)
// 格式: PTTL [KEY]
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

// execPersist: 移除键 [KEY] 的过期时间，使其永久存在。
// 返回值: 1 (成功), 0 (失败，键不存在或本身无过期时间)
// 格式: PERSIST [KEY]
func execPersist(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	_, exists = db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Persist(key)
	db.addAof(utils.ToCmdLine3("persist", args...))
	return protocol.MakeIntReply(1)
}

// execGetKeys: 根据通配符模式查找所有匹配的键。
// 注意: 此命令会遍历整个键空间，可能会阻塞服务器。
// 返回值: 匹配的键列表。
// 格式: KEYS [PATTERN]
func execGetKeys(db *DB, args [][]byte) myredis.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR illegal wildcard")
	}
	result := make([][]byte, 0)
	db.data.ForEach(
		func(key string, val interface{}) bool {
			if !pattern.IsMatch(key) {
				return true
			}
			// 键未过期
			if !db.IsExpired(key) {
				result = append(result, []byte(key))
			}
			return true
		})
	return protocol.MakeMultiBulkReply(result)
}

// execScan: 以游标方式增量迭代数据库中的键。
// 支持 COUNT, MATCH, TYPE 选项。
// 返回值: 新的游标和一批键。
// 格式: SCAN [CURSOR] [MATCH pattern] [COUNT count] [TYPE type]
func execScan(db *DB, args [][]byte) myredis.Reply {
	var count int = 10
	var pattern string = "*"
	var scanType string = ""

	if len(args) > 1 {
		for i := 1; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "count" {
				rawCount, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return protocol.MakeSyntaxErrReply()
				}
				count = rawCount
				i++
			} else if arg == "match" {
				pattern = string(args[i+1])
				i++
			} else if arg == "type" {
				scanType = strings.ToLower(string(args[i+1]))
				i++
			} else {
				return protocol.MakeSyntaxErrReply()
			}
		}
	}
	cursor, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid cursor")
	}
	keysReply, nextCursor := db.data.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("Invalid argument")
	}

	if len(scanType) != 0 {
		for i := 0; i < len(keysReply); {
			// 如果类型不符合，跳过该键
			if getType(db, string(keysReply[i])) != scanType {
				keysReply = append(keysReply[:i], keysReply[i+1:]...)
			} else {
				i++
			}
		}
	}
	result := make([]myredis.Reply, 2)
	result[0] = protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10)))
	result[1] = protocol.MakeMultiBulkReply(keysReply)
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("Del", execDel, writeAllKeys, undoDel, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -1, 1)
	registerCommand("Exists", execExists, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("PTTL", execPTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("Type", execType, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Rename", execRename, prepareRename, undoRename, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Expire", execExpire, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireAt", execExpiredAt, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireTime", execGetExpiredTime, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpire", execPExpire, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpireAt", execPExpiredAt, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpireTime", execGetPExpiredTime, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Persist", execPersist, writeFirstKey, undoExpire, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Keys", execGetKeys, noPrepare, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("Scan", execScan, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
}
