package database

import (
	Dict "myredis/datastruct/dict"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/protocol"
	"strconv"
	"strings"
)

func (db *DB) getAsDict(key string) (Dict.Dict, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return dict, nil
}

func (db *DB) getOrInitDict(key string) (dict Dict.Dict, inited bool, errReply protocol.ErrorReply) {
	dict, errReply = db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if dict == nil {
		dict = Dict.MakeSimple()
		db.PutEntity(key, &database.DataEntity{
			Data: dict,
		})
		inited = true
	}
	return dict, inited, nil
}

// 处理 HSet 命令，设置哈希表字段的值。示例：HSET myhash field1 "Hello"
func execHSet(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])
	value := args[2]

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.Put(member, value)
	return protocol.MakeIntReply(int64(result))
}

func undoHSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	member := string(args[1])
	return rollbackHSetFields(db, key, member)
}

// 处理 HSetNX 命令，当字段不存在时设置哈希表字段的值。示例：HSETNX myhash field2 "World"
func execHSetNX(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])
	value := args[2]

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.PutIfAbsent(member, value)
	return protocol.MakeIntReply(int64(result))
}

// 处理 HGet 命令，获取哈希表字段的值。示例：HGET myhash field1
func execHGet(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeNullBulkReply()
	}

	val, exists := dict.Get(member)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	value, _ := val.([]byte)
	return protocol.MakeBulkReply(value)
}

// 处理 HExists 命令，判断哈希表字段是否存在。示例：HEXISTS myhash field1
func execHExists(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	_, exists := dict.Get(member)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// 处理 HDel 命令，删除哈希表的一个或多个字段。示例：HDEL myhash field1 field2
func execHDel(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	members := make([]string, len(args)-1)
	memberArgs := args[1:]
	for i, memberArg := range memberArgs {
		members[i] = string(memberArg)
	}

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	deleted := 0
	for _, member := range members {
		_, result := dict.Remove(member)
		deleted += result
	}
	// 别忘了全部删除时要删除键
	if dict.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeIntReply(int64(deleted))
}

func undoHDel(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	members := make([]string, len(args)-1)
	memberArgs := args[1:]
	for i, memberArg := range memberArgs {
		members[i] = string(memberArg)
	}
	return rollbackHSetFields(db, key, members...)
}

// 处理 HLen 命令，获取哈希表字段的数量。示例：HLEN myhash
func execHLen(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(dict.Len()))
}

// 处理 HStrlen 命令，获取哈希表字段值的字符串长度。示例：HSTRLEN myhash field1
func execHStrlen(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	val, exists := dict.Get(member)
	if exists {
		value, _ := val.([]byte)
		return protocol.MakeIntReply(int64(len(value)))
	}
	return protocol.MakeIntReply(0)
}

// 处理 HMSet 命令，同时设置哈希表多个字段的值。示例：HMSET myhash field1 "Hello" field2 "World"
func execHMSet(db *DB, args [][]byte) myredis.Reply {
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}

	key := string(args[0])
	size := (len(args) - 1) / 2
	members := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		members[i] = string(args[2*i+1])
		values[i] = args[2*i+2]
	}

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	for i, member := range members {
		value := values[i]
		dict.Put(member, value)
	}
	return protocol.MakeOkReply()
}

func undoHMSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	members := make([]string, size)
	for i := 0; i < size; i++ {
		members[i] = string(args[2*i+1])
	}
	return rollbackHSetFields(db, key, members...)
}

// 处理 HMGet 命令，获取哈希表一个或多个字段的值。示例：HMGET myhash field1 field2
func execHMGet(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	size := len(args) - 1

	members := make([]string, size)
	for i := 0; i < size; i++ {
		members[i] = string(args[i+1])
	}

	result := make([][]byte, size)
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeMultiBulkReply(result)
	}

	for i, member := range members {
		val, ok := dict.Get(member)
		if !ok {
			result[i] = nil
		} else {
			value, _ := val.([]byte)
			result[i] = value
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

// 处理 HGetAll 命令，获取哈希表中所有的字段和值。示例：HGETALL myhash
func execHGetAll(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	size := dict.Len()

	result := make([][]byte, size*2)
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		result[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result[:i])
}

// 处理 HKeys 命令，获取哈希表中所有的字段名。示例：HKEYS myhash
func execHKeys(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	members := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		members[i] = []byte(key)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(members[:i])
}

// 处理 HVals 命令，获取哈希表中所有的值。示例：HVALS myhash
func execHVals(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	values := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		values[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(values[:i])
}

// 处理 HIncrBy 命令，将哈希表字段的整数值加上指定增量。示例：HINCRBY myhash counter 5
func execHIncrBy(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])
	delta, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(member)
	if !exists {
		dict.Put(member, args[2])
		return protocol.MakeBulkReply(args[2])
	}
	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		protocol.MakeErrReply("ERR hash value is not an integer")
	}

	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	dict.Put(key, bytes)
	return protocol.MakeBulkReply(bytes)
}

func undoHIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	member := string(args[1])
	return rollbackHSetFields(db, key, member)
}

// 处理 HIncrByFloat 命令，将哈希表字段的浮点数值加上指定增量。示例：HINCRBYFLOAT myhash balance 10.5
func execHIncrByFloat(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])
	delta, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(member)
	if !exists {
		dict.Put(member, args[2])
		return protocol.MakeBulkReply(args[2])
	}
	val, err := strconv.ParseFloat(string(value.([]byte)), 64)
	if err != nil {
		protocol.MakeErrReply("ERR hash value is not an integer")
	}

	val += delta
	bytes := []byte(strconv.FormatFloat(val, 'f', -1, 64))
	dict.Put(key, bytes)
	return protocol.MakeBulkReply(bytes)
}

// 处理 HRandField 命令，随机返回哈希表中的一个或多个字段，可选返回值。示例：HRANDFIELD myhash 2 WITHVALUES
func execHRandMember(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	count := 1
	withValues := 0

	if len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hrandfield' command")
	}

	if len(args) == 3 {
		if strings.ToLower(string(args[2])) == "withvalues" {
			withValues = 1
		} else {
			return protocol.MakeSyntaxErrReply()
		}
	}

	if len(args) >= 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = int(count64)
	}

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	if count > 0 {
		members := dict.RandomDistinctKeys(count)
		size := len(members)
		if withValues == 0 {
			result := make([][]byte, size)
			for i, member := range members {
				result[i] = []byte(member)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2*size)
			for i, member := range members {
				result[2*i] = []byte(member)
				value, _ := dict.Get(member)
				result[2*i+1] = value.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	} else if count < 0 {
		members := dict.RandomDistinctKeys(-count)
		size := len(members)
		if withValues == 0 {
			result := make([][]byte, size)
			for i, member := range members {
				result[i] = []byte(member)
			}
			return protocol.MakeMultiBulkReply(result)
		} else {
			result := make([][]byte, 2*size)
			for i, member := range members {
				result[2*i] = []byte(member)
				value, _ := dict.Get(member)
				result[2*i+1] = value.([]byte)
			}
			return protocol.MakeMultiBulkReply(result)
		}
	}
	return protocol.MakeEmptyMultiBulkReply()
}

// 处理 HScan 命令，增量式迭代哈希表中的字段。示例：HSCAN myhash 0 MATCH field* COUNT 10
func execHScan(db *DB, args [][]byte) myredis.Reply {
	var count int = 10
	var pattern string = "*"
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "count" {
				tempCount, err := strconv.Atoi(string(arg[i+1]))
				if err != nil {
					return protocol.MakeSyntaxErrReply()
				}
				count = tempCount
				i++
			} else if arg == "match" {
				pattern = string(arg[i+1])
				i++
			} else {
				return protocol.MakeSyntaxErrReply()
			}
		}
	}
	if len(args) < 2 {
		return protocol.MakeSyntaxErrReply()
	}

	key := string(args[0])
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeNullBulkReply()
	}

	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid cursor")
	}

	replys, nextCursor := dict.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("Invalid argument")
	}

	result := make([]myredis.Reply, 2)
	result[0] = protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10)))
	result[1] = protocol.MakeMultiBulkReply(replys)
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("HSet", execHSet, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HGet", execHGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HGetAll", execHGetAll, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
	registerCommand("HExists", execHExists, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HKeys", execHKeys, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("HVals", execHVals, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)

	registerCommand("HDel", execHDel, writeFirstKey, undoHDel, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)

	registerCommand("HLen", execHLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HStrlen", execHStrlen, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)

	registerCommand("HMSet", execHMSet, writeFirstKey, undoHMSet, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HMGet", execHMGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)

	registerCommand("HIncrBy", execHIncrBy, writeFirstKey, undoHIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HIncrByFloat", execHIncrByFloat, writeFirstKey, undoHIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)

	registerCommand("HRandField", execHRandMember, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagRandom, redisFlagReadonly}, 1, 1, 1)
	registerCommand("HScan", execHScan, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
}
