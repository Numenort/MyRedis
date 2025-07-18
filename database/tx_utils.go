package database

import "myredis/lib/utils"

// 获取 write keys / read keys
// 用于 multi 事务时，使用乐观锁进行加锁

// 在命令实际执行前保存当前状态用于回滚

// 命令只读取其第一个参数作为键
// for example: GET <key>, EXISTS <key>, TTL <key>
func readFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return nil, []string{key}
}

// 命令会读取其所有参数作为键
// for example: MGET key1 key2 key3
func readAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return nil, keys
}

// 命令只写入其第一个参数作为键
// for example: SET <key> <value>, DEL <key>, INCR <key>
func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

// 命令会写入其所有参数作为键
// for example: 自定义的批量删除命令 MDELETE key1 key2 key3
func writeAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return keys, nil
}

// 命令不涉及对任何特定键的读写操作
// for example: PING, ECHO, MULTI, EXEC, DISCARD
func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}

// 集合的计算操作（如 SINTER, SUNION, SDIFF）识别所有作为输入集合的键
func prepareSetCalculate(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return nil, keys
}

// 集合计算并存储结果的操作（如 SINTERSTORE, SUNIONSTORE, SDIFFSTORE）识别写入的目标键和读取的源键
// for example: SINTERSTORE my_intersection_set set1 set2 set3
func prepareSetCalculateStore(args [][]byte) ([]string, []string) {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}
	return []string{dest}, keys
}

// 操作回滚 / 撤销命令生成函数

// 为操作第一个键的命令生成回滚命令
func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(db, key)
}

// 对给定的一个或多个键生成回滚命令
func rollbackGivenKeys(db *DB, keys ...string) []CmdLine {
	var undoCmdLine [][][]byte
	for _, key := range keys {
		_, ok := db.GetEntity(key)
		if !ok {
			undoCmdLine = append(undoCmdLine,
				utils.ToCmdLine("DEL", key))
		} else {
			undoCmdLine = append(undoCmdLine,
				utils.ToCmdLine("DEL", key),
				toTTLCmd(db, key).Args,
			)
		}
	}
	return undoCmdLine
}

// 回滚 Set 成员的相关函数，包含 SADD、SREM 以及 SPop
func rollbackSetMembers(db *DB, key string, members ...string) []CmdLine {
	var undoCmdLines [][][]byte
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return nil
	}
	// 原始操作创建了这个集合，回滚需要删除该键
	if set == nil {
		undoCmdLines = append(undoCmdLines,
			utils.ToCmdLine("DEL", key),
		)
		return undoCmdLines
	}
	for _, member := range members {
		ok := set.Has(member)
		// 原来不在集合内，即执行 SADD 命令，需要移除
		if !ok {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("SREM", key, member),
			)
			// 原来在集合内，即执行 SREM 命令，需要加入
		} else {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("SADD", key, member),
			)
		}
	}
	return undoCmdLines
}
