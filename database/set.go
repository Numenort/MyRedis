package database

import (
	HashSet "myredis/datastruct/set"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/protocol"
	"strconv"
)

func (db *DB) getAsSet(key string) (*HashSet.Set, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return set, nil
}

func (db *DB) getOrInitSet(key string) (set *HashSet.Set, inited bool, errReply protocol.ErrorReply) {
	set, errReply = db.getAsSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if set == nil {
		set = HashSet.Make()
		db.PutEntity(
			key,
			&database.DataEntity{
				Data: set,
			},
		)
		inited = true
		return set, inited, nil
	}
	return set, inited, nil
}

// 将成员插入集合
func execSAdd(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sadd' command")
	}

	key := string(args[0])
	members := args[1:]

	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}
	count := 0
	for _, member := range members {
		count += set.Add(string(member))
	}
	return protocol.MakeIntReply(int64(count))
}

// 检查成员是否在集合内，返回成员在集合内的数量
func execSIsMember(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sismember' command")
	}

	key := string(args[0])
	member := string(args[1])

	set, _, errReply := db.getOrInitSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}

	has := set.Has(member)
	if has {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// 从集合中移除成员，返回移除的数量
func execSRem(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'srem' command")
	}

	key := string(args[0])
	members := args[1:]

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}

	count := 0
	for _, member := range members {
		count += set.Remove(string(member))
	}

	if set.Len() == 0 {
		db.Remove(key)
	}
	return protocol.MakeIntReply(int64(count))
}

// 从集合中移除成员，按照数量随机移除，返回被移除的 key
func execSPop(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'spop' command")
	}

	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}

	count := 0
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || count64 <= 0 {
			return protocol.MakeErrReply("ERR value is out of range, must be positive")
		}
		count = int(count64)
	}

	if count > set.Len() {
		count = set.Len()
	}

	members := set.RandomDistinctKeys(count)
	result := make([][]byte, len(members))
	for i, member := range members {
		set.Remove(member)
		result[i] = []byte(member)
	}

	return protocol.MakeMultiBulkReply(result)
}

// 返回集合中的成员数量
func execSCard(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'scard' command")
	}

	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(set.Len()))
}

// 返回集合里的全部成员
func execSMemebers(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'smembers' command")
	}

	key := string(args[0])

	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		result[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

// 将集合转化为消息回复
func set2reply(set *HashSet.Set) myredis.Reply {
	result := make([][]byte, set.Len())
	i := 0
	set.ForEach(func(member string) bool {
		result[i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

// 执行多个集合的交集操作
func execSInter(db *DB, args [][]byte) myredis.Reply {
	sets := make([]*HashSet.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set.Len() == 0 {
			return &protocol.EmptyMultiBulkReply{}
		}
		sets = append(sets, set)
	}
	result := HashSet.Intersect(sets...)
	return set2reply(result)
}

// 执行多个集合的交集操作，并将结果存入 key 对应的 set
func execSInterStore(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sinterstore' command")
	}

	dest := string(args[0])
	sets := make([]*HashSet.Set, 0, len(args)-1)

	for i := 1; i < len(args); i++ {
		key := string(args[i])
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if set.Len() == 0 {
			return protocol.MakeIntReply(0)
		}
		sets = append(sets, set)
	}

	result := HashSet.Intersect(sets...)
	// 清理过时时间（如果设置）确保 key 的数据行为一致
	db.Remove(dest)
	db.PutEntity(dest, &database.DataEntity{
		Data: result,
	})
	return protocol.MakeIntReply(int64(result.Len()))
}

// 执行多个集合的并集操作
func execSUnion(db *DB, args [][]byte) myredis.Reply {
	sets := make([]*HashSet.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}
	result := HashSet.Union(sets...)
	return set2reply(result)
}

// 执行多个集合的并集操作，并将结果存入 key 对应的 set
func execSUnionStore(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sunionstore' command")
	}

	dest := string(args[0])
	sets := make([]*HashSet.Set, 0, len(args)-1)

	for i := 1; i < len(args); i++ {
		key := string(args[i])
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}

	result := HashSet.Union(sets...)
	// 清理过时时间（如果设置）确保 key 的数据行为一致
	db.Remove(dest)
	db.PutEntity(dest, &database.DataEntity{
		Data: result,
	})

	return protocol.MakeIntReply(int64(result.Len()))
}

// 执行多个集合的差集操作
func execSDiff(db *DB, args [][]byte) myredis.Reply {
	sets := make([]*HashSet.Set, 0, len(args))
	for _, arg := range args {
		key := string(arg)
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}
	result := HashSet.Diff(sets...)
	return set2reply(result)
}

// 执行多个集合的差集操作，并将结果存入 key 对应的 set
func execSDiffStore(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sdiffstore' command")
	}

	dest := string(args[0])
	sets := make([]*HashSet.Set, 0, len(args)-1)

	for i := 1; i < len(args); i++ {
		key := string(args[i])
		set, errReply := db.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, set)
	}

	result := HashSet.Diff(sets...)
	// 清理过时时间（如果设置）确保 key 的数据行为一致
	db.Remove(dest)
	if result.Len() == 0 {
		return protocol.MakeIntReply(0)
	}
	db.PutEntity(dest, &database.DataEntity{
		Data: result,
	})
	return protocol.MakeIntReply(int64(result.Len()))
}
