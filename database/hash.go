package database

import (
	Dict "myredis/datastruct/dict"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/protocol"
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

func init() {
	registerCommand("HSet", execHSet, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HGet", execHGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HExists", execHExists, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)

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
}
