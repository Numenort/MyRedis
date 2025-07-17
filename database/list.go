package database

import (
	"fmt"
	List "myredis/datastruct/list"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
	"strconv"
	"strings"
)

// 从数据库中获取指定 key 对应的实体，确保该实体的数据类型是 List
func (db *DB) getAsList(key string) (List.List, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	list, ok := entity.Data.(List.List)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return list, nil
}

// 从数据库中获取指定 key 对应的实体，如果不存在就创建一个
func (db *DB) getOrInitList(key string) (list List.List, isNew bool, errReply protocol.ErrorReply) {
	list, errReply = db.getAsList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if list == nil {
		list = List.NewQuickList()
		db.PutEntity(
			key,
			&database.DataEntity{
				Data: list,
			},
		)
		isNew = true
	}
	return list, isNew, nil
}

// 获取 list 指定 index 的值
func execLIndex(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lindex' command")
	}
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeNullBulkReply()
	}

	size := list.Len() // assert: size > 0
	if index < -1*size {
		return protocol.MakeNullBulkReply()
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return protocol.MakeNullBulkReply()
	}

	val, _ := list.Get(index).([]byte)
	return protocol.MakeBulkReply(val)
}

// 获取 list 的长度
func execLLen(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'llen' command")
	}
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	size := int64(list.Len())
	return protocol.MakeIntReply(size)
}

// 从左侧开始移除 key 对应的 List 的成员  LPop key count
func execLPop(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lpop' command")
	}
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeNullBulkReply()
	}
	// 如果指定了 POP 的个数
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count := int(count64)
		if count > list.Len() {
			count = list.Len()
		}
		vals := make([][]byte, count)
		for i := 0; i < count; i++ {
			val := list.Remove(0).([]byte)
			vals[i] = val
		}
		if list.Len() == 0 {
			db.Remove(key)
		}
		return protocol.MakeMultiBulkReply(vals)
	}
	// 只移除一个
	val, _ := list.Remove(0).([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	return protocol.MakeBulkReply(val)
}

var lPushCmd = []byte("LPUSH")

// 在执行 LPOP 命令之前先记录即将被删除的元素，然后生成相应的撤销命令
func undoLPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	// 如果指定撤回数量
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return nil
		}
		count := int(count64)
		if count > list.Len() {
			count = list.Len()
		}

		elements := make([][]byte, count)
		vals := list.Range(0, count)
		for i := 0; i < count; i++ {
			elements[count-i-1] = vals[i].([]byte)
		}
		cmd := CmdLine{lPushCmd, args[0]}
		cmd = append(cmd, elements...)
		return []CmdLine{cmd}
	}
	// 撤回单个命令
	element, _ := list.Get(0).([]byte)
	return []CmdLine{
		{
			lPushCmd,
			args[0],
			element,
		},
	}
}

// 向 key 对应的 list 中插入对应的值，返回 list 的长度
func execLPush(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lpush' command")
	}
	key := string(args[0])
	values := args[1:]

	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	for _, value := range values {
		list.Insert(0, value)
	}

	return protocol.MakeIntReply(int64(list.Len()))
}

// 在执行 LPUSH 命令之前先记录即将被插入的元素，然后生成相应的撤销命令
func undoLPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])

	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)

	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("LPOP", key))
	}
	return cmdLines
}

// 在 list 存在的情况下才向 key 对应的 list 中从左侧插入对应的值，返回 list 的长度
func execLPushX(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lpushX' command")
	}

	key := string(args[0])
	values := args[1:]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	for _, value := range values {
		list.Insert(0, value)
	}
	return protocol.MakeIntReply(int64(list.Len()))
}

// 获取指定范围内的 list 的值 [LRANGE key start stop]
func execLRange(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lrange' command")
	}
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if list == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// 解析起始、结束位置
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)

	stop64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop := int(stop64)

	size := list.Len()
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	rawValues := list.Range(start, stop)
	result := make([][]byte, len(rawValues))
	for i, rawValue := range rawValues {
		result[i] = rawValue.([]byte)
	}
	return protocol.MakeMultiBulkReply(result)
}

// 从 key 对应的 list 中删除一定数量的等于指定值的元素
//
// 0: 移除全部元素; >0: 正向移除指定数量元素 <0: 反向移除指定数量元素
func execLRem(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lrem' command")
	}
	key := string(args[0])
	value := args[2]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)

	var removed int
	// 移除
	if count == 0 {
		removed = list.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		})
	} else if count > 0 {
		removed = list.RemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		}, count)
	} else {
		removed = list.ReverseRemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		}, -count)
	}

	// 全部移除，删掉 key
	if list.Len() == 0 {
		db.Remove(key)
	}
	return protocol.MakeIntReply(int64(removed))
}

// 在 list 的指定位置设置对应值		[lSET key index value]
func execLSet(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lset' command")
	}

	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	index := int(index64)
	value := args[2]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeErrReply("ERR No Such Key")
	}

	size := list.Len()
	if index < -1*size {
		return protocol.MakeErrReply("ERR index out of range")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return protocol.MakeErrReply("ERR index out of range")
	}

	list.Set(index, value)
	return protocol.MakeOkReply()
}

// 在执行 LSET 命令前存储原始值，生成回滚的命令
func undoLSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return nil
	}
	index := int(index64)

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil {
		return nil
	}

	size := list.Len()
	if index < -1*size {
		return nil
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return nil
	}

	// 获取设置前的值
	value, _ := list.Get(index).([]byte)
	// 设置为原来的值
	return []CmdLine{
		{
			[]byte("LSET"),
			args[0],
			args[1],
			value,
		},
	}
}

// 从右侧开始移除 key 对应的 List 的成员  RPop key count
func execRPop(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpop' command")
	}

	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeNullBulkReply()
	}

	// 如果设置 POP 的数量
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count := int(count64)
		if count > list.Len() {
			count = list.Len()
		}
		vals := make([][]byte, count)
		for i := 0; i < count; i++ {
			val := list.RemoveLast().([]byte)
			vals[i] = val
		}
		// 如果全删完了
		if list.Len() == 0 {
			db.Remove(key)
		}
		return protocol.MakeMultiBulkReply(vals)
	}
	// 未设置删除数量
	val, _ := list.RemoveLast().([]byte)
	if list.Len() == 0 {
		db.Remove(key)
	}
	return protocol.MakeBulkReply(val)
}

var rPushCmd = []byte("RPUSH")

func undoRPop(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	list, errReply := db.getAsList(key)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	if len(args) == 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return nil
		}
		count := int(count64)
		if count > list.Len() {
			count = list.Len()
		}
		elements := make([][]byte, count)
		vals := list.Range(list.Len()-count, list.Len())
		for i := 0; i < count; i++ {
			elements[i] = vals[i].([]byte)
		}
		cmd := CmdLine{rPushCmd, args[0]}
		cmd = append(cmd, elements...)
		return []CmdLine{cmd}
	}
	element, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
	}
}

func prepareRPopLPush(args [][]byte) ([]string, []string) {
	return []string{
		string(args[0]),
		string(args[1]),
	}, nil
}

// 移除 List A 的最后一个元素，插入 List B 的第一个位置
func execRPopLPush(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpoplpush' command")
	}
	sourceKey := string(args[0])
	destKey := string(args[1])

	sourceList, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return errReply
	}
	if sourceList == nil {
		return protocol.MakeNullBulkReply()
	}

	destList, _, errReply := db.getOrInitList(destKey)
	if errReply != nil {
		return errReply
	}

	// RPop
	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		db.Remove(sourceKey)
	}

	return protocol.MakeBulkReply(val)
}

var lPopCmd = []byte("LPOP")

// 执行 RPOPLPUSH 前记录相应的操作，生成回滚命令
func undoRPopLPush(db *DB, args [][]byte) []CmdLine {
	sourceKey := string(args[0])
	list, errReply := db.getAsList(sourceKey)
	if errReply != nil {
		return nil
	}
	if list == nil || list.Len() == 0 {
		return nil
	}
	element, _ := list.Get(list.Len() - 1).([]byte)
	return []CmdLine{
		{
			rPushCmd,
			args[0],
			element,
		},
		{
			lPopCmd,
			args[1],
		},
	}
}

// 将值插入 key 对应的 List 的尾部 （即右侧插入）
func execRPush(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}
	key := string(args[0])
	values := args[1:]

	list, _, errReply := db.getOrInitList(key)
	if errReply != nil {
		return errReply
	}

	for _, value := range values {
		list.Add(value)
	}

	return protocol.MakeIntReply(int64(list.Len()))
}

// 记录插入的个数，生成相应数量的 RPOP 命令用于回滚
func undoRPush(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	count := len(args) - 1
	cmdLines := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine("RPOP", key))
	}
	return cmdLines
}

// 在 list 存在的情况下才向 key 对应的 list 中从右侧插入对应的值，返回 list 的长度
func execRPushX(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpushX' command")
	}

	key := string(args[0])
	values := args[1:]

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeIntReply(0)
	}

	for _, value := range values {
		list.Add(value)
	}
	return protocol.MakeIntReply(int64(list.Len()))
}

// 保留指定范围内的元素，删除范围外的所有元素
func execLTrim(db *DB, args [][]byte) myredis.Reply {
	size := len(args)
	if size != 3 {
		return protocol.MakeErrReply(fmt.Sprintf("ERR wrong number of arguments (given %d, expected 3)", size))
	}
	key := string(args[0])

	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	start := int(start64)

	end64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end := int(end64)

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeOkReply()
	}

	len := list.Len()
	if start < 0 {
		start = len + start
	}
	if end < 0 {
		end = len + end
	}

	leftCount := start
	rightCount := len - end - 1

	for i := 0; i < leftCount && list.Len() > 0; i++ {
		list.Remove(0)
	}

	for i := 0; i < rightCount && list.Len() > 0; i++ {
		list.RemoveLast()
	}

	if list.Len() == 0 {
		db.Remove(key)
	}
	return protocol.MakeOkReply()
}

// LINSERT mylist BEFORE d 0
func execLInsert(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'linsert' command")
	}
	key := string(args[0])

	list, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}

	if list == nil {
		return protocol.MakeIntReply(0)
	}

	pos := strings.ToLower(string(args[1]))
	if pos != "before" && pos != "after" {
		return protocol.MakeErrReply("ERR syntax error")
	}

	pivot := string(args[2])
	index := -1
	// 遍历寻找 pivot
	list.ForEach(func(i int, v interface{}) bool {
		if string(v.([]byte)) == pivot {
			index = i
			return false
		}
		return true
	})
	if index == -1 {
		return protocol.MakeIntReply(-1)
	}

	val := args[3]
	if pos == "before" {
		list.Insert(index, val)
	} else if pos == "after" {
		list.Insert(index+1, val)
	}

	return protocol.MakeIntReply(int64(list.Len()))
}

func init() {
	registerCommand("LIndex", execLIndex, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("LLen", execLLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("LPop", execLPop, writeFirstKey, undoLPop, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("LPush", execLPush, writeFirstKey, undoLPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("LPushX", execLPushX, writeFirstKey, undoLPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("LRange", execLRange, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("LSet", execLSet, writeFirstKey, undoLSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("RPop", execRPop, writeFirstKey, undoRPop, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	// 遵循 Redis 的约定，将源键作为主要键来标记
	registerCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("RPush", execRPush, writeFirstKey, undoRPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("LTrim", execLTrim, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("LInsert", execLInsert, writeFirstKey, rollbackFirstKey, 5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
}
