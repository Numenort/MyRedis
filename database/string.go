package database

import (
	"math/bits"
	"myredis/datastruct/bitmap"
	"myredis/interface/database"
	"myredis/lib/utils"
	"myredis/myredis"
	"myredis/protocol"
	"strconv"
	"strings"
	"time"
)

const unlimitedTTL int64 = 0

/*
upsertPolicy:

	SET 命令默认行为: SET 命令没有指定 NX 或 XX 选项
	update or insert: 如果键 key 不存在，就创建一个新键并设置其值; 如果键 key 已经存在，就更新它的值。

insertPolicy:

	SET 命令中包含 NX (Not e eXists) 选项时
	当 key 不存在时，设置它的值; 如果 key 已经存在，则不执行任何操作。

updatePolicy:

	SET 命令中包含 XX (eXists) 选项时
	当 key 已经存在时，更新它的值。如果 key 不存在，则不执行任何操作。
*/
const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

// 从数据库中获取指定 key 对应的实体，确保该实体的数据是字节切片(String要求)
func (db *DB) getAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return bytes, nil
}

// ******************** GET Functions ********************

// Get 命令，获取 key 对应的字符串的值
func execGet(db *DB, args [][]byte) myredis.Reply {
	// 获取 key
	key := string(args[0])
	// 获取 val
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	// 访问到不存在的 key
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(bytes)
}

// GETEX 命令，获取 key 的字符串值，同时设置新的过期时间或移除过期时间 (PERSIST)
func execGetEX(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	ttl := unlimitedTTL
	if err != nil {
		return err
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}
	for i := 1; i < len(args); i++ {
		arg := string(args[i])
		// 单位为 seconds
		if arg == "EX" {
			// 如果 ttl 已经被设置过
			if ttl != unlimitedTTL {
				return &protocol.SyntaxErrReply{}
			}
			// 参数数量不够
			if i+1 >= len(args) {
				return &protocol.SyntaxErrReply{}
			}
			// int64类型
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.SyntaxErrReply{}
			}
			// ttl 不能小于 0
			if ttlArg <= 0 {
				return protocol.MakeErrReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg * 1000
			i++
		} else if arg == "PX" { // 单位为 milliseconds
			if ttl != unlimitedTTL {
				return &protocol.SyntaxErrReply{}
			}
			if i+1 >= len(args) {
				return &protocol.SyntaxErrReply{}
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return &protocol.SyntaxErrReply{}
			}
			if ttlArg <= 0 {
				return protocol.MakeErrReply("ERR invalid expire time in getex")
			}
			ttl = ttlArg
			i++
		} else if arg == "PERSIST" { // 持久化，移除过期时间
			if ttl != unlimitedTTL { // PERSIST Cannot be used with EX | PX
				return &protocol.SyntaxErrReply{}
			}
			if i+1 > len(args) {
				return &protocol.SyntaxErrReply{}
			}
			// 移除过期时间
			db.Persist(key)
		}
	}

	// 如果设置了过期时间，加入过期操作
	if len(args) > 1 {
		if ttl != unlimitedTTL {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			// TODO: Aof 持久化
		} else {
			db.Persist(key)
		}
	}
	return protocol.MakeBulkReply(bytes)
}

// ******************** SET Functions ********************

// SET 命令，设置 key 的 string 值
func execSet(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	value := args[1]
	ttl := unlimitedTTL
	policy := upsertPolicy

	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToUpper(string(args[i]))
			// 插入策略
			if arg == "NX" {
				if policy == updatePolicy {
					return &protocol.SyntaxErrReply{}
				}
				policy = insertPolicy
				// 更新策略
			} else if arg == "XX" {
				if policy == insertPolicy {
					return &protocol.SyntaxErrReply{}
				}
				policy = updatePolicy
				// 超时设置（秒）
			} else if arg == "EX" {
				// ttl 被更新了
				if ttl != unlimitedTTL {
					return &protocol.SyntaxErrReply{}
				}
				// 没设置超时时长
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				// 超时时长不能为负数
				if ttlArg <= 0 {
					return protocol.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg * 1000
				i++
				// 超时设置（毫秒）
			} else if arg == "PX" {
				if ttl != unlimitedTTL {
					return &protocol.SyntaxErrReply{}
				}
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				if ttlArg <= 0 {
					return protocol.MakeErrReply("ERR invalid expire time in set")
				}
				ttl = ttlArg
				i++
				// 未知命令
			} else {
				return &protocol.SyntaxErrReply{}
			}
		}
	}

	// 键值的实体存储
	entity := &database.DataEntity{
		Data: value,
	}

	// 根据不同的更新策略更新内存数据库
	var res int
	switch policy {
	case upsertPolicy:
		db.PutEntity(key, entity)
		res = 1
	case insertPolicy:
		res = db.data.PutIfAbsentWithLock(key, entity)
	case updatePolicy:
		res = db.data.PutIfExistsWithLock(key, entity)
	}
	if res > 0 {
		// 设置了过时时间
		if ttl != unlimitedTTL {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expireTime)
			// TODO: 持久化操作
		} else {
			db.Persist(key)
		}
	}
	// 操作成功
	if res > 0 {
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
}

// SETNX 命令，key 不存在时，设置值
// eg: SETNX key value
func execSetNX(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	res := db.data.PutIfAbsentWithLock(key, entity)
	// TODO: 加入恢复日志
	return protocol.MakeIntReply(int64(res))
}

// SETEX 命令，设置过时时间
// eg : SETEX key seconds value
func execSetEX(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrReply("ERR invalid expire time in setex")
	}
	ttl := ttlArg * 1000

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
	db.Expire(key, expireTime)
	return &protocol.OkReply{}
}

// 过期时间为 millisecond 单位
func execPSetEX(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrReply("ERR invalid expire time in setex")
	}

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
	db.Expire(key, expireTime)
	return &protocol.OkReply{}
}

// ******************** MSET Functions ********************

// 生成 MSET 所需的键值对
func prepareMSet(args [][]byte) ([]string, []string) {
	size := len(args) / 2
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[i*2])
	}
	return keys, nil
}

// 撤销 MSet 操作需要执行的命令
func undoMSet(db *DB, args [][]byte) []CmdLine {
	// 只需要撤回写键
	writekeys, _ := prepareMSet(args)
	return rollbackGivenKeys(db, writekeys...)
}

// 执行 MSet 命令
func execMSet(db *DB, args [][]byte) myredis.Reply {
	if len(args)%2 != 0 {
		return &protocol.SyntaxErrReply{}
	}

	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)

	// 获取键值对
	for i := 0; i < size; i++ {
		keys[i] = string(args[i*2])
		values[i] = args[i*2+1]
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	return &protocol.OkReply{}
}

// 准备 MGET 命令的 readKeys
func prepareMGet(args [][]byte) ([]string, []string) {
	key := make([]string, len(args))
	for i, k := range args {
		key[i] = string(k)
	}
	return nil, key
}

// 执行 MGET 命令
func execMGet(db *DB, args [][]byte) myredis.Reply {
	keys := make([]string, len(args))

	for i, k := range args {
		keys[i] = string(k)
	}

	results := make([][]byte, len(args))
	for i, key := range keys {
		bytes, err := db.getAsString(key)
		if err != nil {
			_, isWrongType := err.(*protocol.WrongTypeErrReply)
			// 即使某个键存在但不是字符串类型，MGET 也会返回 nil
			if isWrongType {
				results[i] = nil
				continue
			} else {
				return err
			}
		}
		results[i] = bytes
	}
	return protocol.MakeMultiBulkReply(results)
}

// key 不存在时，设置值
func execMSetNX(db *DB, args [][]byte) myredis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	size := len(args) / 2
	values := make([][]byte, size)
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[i*2])
		values[i] = args[i*2+1]
	}

	for _, key := range keys {
		_, exists := db.GetEntity(key)
		// key 存在
		if exists {
			return protocol.MakeIntReply(0)
		}
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}
	return protocol.MakeIntReply(1)
}

// 设置新值，并返回旧值
func execGetSet(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	value := args[1]

	// 获取旧的值
	oldValue, err := db.getAsString(key)
	if err != nil {
		return err
	}

	db.PutEntity(key, &database.DataEntity{Data: value})
	db.Persist(key)

	// 如果旧值不存在，返回 null 回复
	if oldValue == nil {
		return new(protocol.NullBulkReply)
	}

	return protocol.MakeBulkReply(oldValue)
}

// 获取 key 对应的 value，并删除 value
func execGetDel(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}

	if old == nil {
		return new(protocol.NullBulkReply)
	}
	db.Remove(key)

	return protocol.MakeBulkReply(old)
}

// 将 key 对应的值自增
func execIncr(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	// 先获取值，再自增
	val_bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if val_bytes != nil {
		val, err := strconv.ParseInt(string(val_bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{Data: []byte(strconv.FormatInt(val+1, 10))})
		return protocol.MakeIntReply(val + 1)
	}
	// 如果值不存在，那么设置为 1
	db.PutEntity(key, &database.DataEntity{Data: []byte("1")})

	return protocol.MakeIntReply(1)
}

// 将 key 对应的值增加 value
func execIncrBy(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	rawData := string(args[1])
	data, err := strconv.ParseInt(rawData, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	val_bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	// 存在对应的值，按 value 自增
	if val_bytes != nil {
		val, err := strconv.ParseInt(string(val_bytes), 10, 64)
		// 数字解析错误
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{Data: []byte(strconv.FormatInt(val+data, 10))})
		return protocol.MakeIntReply(val + data)
	}

	// 如果值不存在，那么设置为 value
	db.PutEntity(key, &database.DataEntity{Data: args[1]})
	return protocol.MakeIntReply(data)
}

// 将 key 对应的值增加 value （Float 类型）
func execIncrByFloat(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	rawData := string(args[1])
	data, err := strconv.ParseFloat(rawData, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	val_bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if val_bytes != nil {
		val, err := strconv.ParseFloat(string(val_bytes), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		resultBytes := []byte(strconv.FormatFloat(val+data, 'f', -1, 64))
		db.PutEntity(key, &database.DataEntity{Data: resultBytes})
		return protocol.MakeBulkReply(resultBytes)
	}
	db.PutEntity(key, &database.DataEntity{Data: args[1]})
	return protocol.MakeBulkReply(args[1])
}

// 将 key 对应的值自减
func execDecr(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	val_bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if val_bytes != nil {
		val, err := strconv.ParseInt(string(val_bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{Data: []byte(strconv.FormatInt(val-1, 10))})
		return protocol.MakeIntReply(val - 1)
	}
	db.PutEntity(key, &database.DataEntity{Data: []byte("-1")})
	return protocol.MakeIntReply(-1)
}

// 将 key 对应的值自减少 value
func execDecrBy(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	rawData := string(args[1])
	data, err := strconv.ParseInt(rawData, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	val_bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	// 如果 key 的值存在
	if val_bytes != nil {
		val, err := strconv.ParseInt(string(val_bytes), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{Data: []byte(strconv.FormatInt(val-data, 10))})
		return protocol.MakeIntReply(val - data)
	}
	// key 的值不存在
	valueStr := strconv.FormatInt(-data, 10)
	db.PutEntity(key, &database.DataEntity{Data: []byte(valueStr)})
	return protocol.MakeIntReply(-data)
}

// 返回 value 的字符串值的字节长度
func execStrLen(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(len(bytes)))
}

// 将一个字符串追加到现有字符串值的末尾
func execAppend(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	bytes = append(bytes, args[1]...)
	db.PutEntity(key, &database.DataEntity{
		Data: bytes,
	})
	return protocol.MakeIntReply(int64(len(bytes)))
}

// 指定 key 对应的字符串中，从某个偏移量开始覆盖写入一段新的字节数据
func execSetRange(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	// 解析偏移量
	offset, errReply := strconv.ParseInt(string(args[1]), 10, 64)
	if errReply != nil {
		return protocol.MakeErrReply(errReply.Error())
	}
	value := args[2]
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	bytesLen := int64(len(bytes))
	// 偏移量大于当前字符串长度，加入 0
	if offset > bytesLen {
		diff := offset - bytesLen
		diffArr := make([]byte, diff)
		bytes = append(bytes, diffArr...)
		bytesLen = int64(len(bytes))
	}
	for i := 0; i < len(value); i++ {
		// 计算插入后的位置是否超过字符串长度
		idx := offset + int64(i)
		// 附加
		if idx >= bytesLen {
			bytes = append(bytes, value[i])
		} else {
			bytes[idx] = value[i]
		}
	}
	db.PutEntity(key, &database.DataEntity{
		Data: bytes,
	})
	return protocol.MakeIntReply(int64(len(bytes)))
}

// 按范围截取 key 对应的字符串的内容
func execGetRange(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	// 获取起始与结束的偏移量
	startIndex, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	endIndex, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		return protocol.MakeNullBulkReply()
	}
	bytesLen := int64(len(bytes))
	// 转换为 go 中的切片
	begin, end := utils.ConvertRange(startIndex, endIndex, bytesLen)
	if begin < 0 {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(bytes[begin:end])
}

// 设置或清除存储在字符串键中的特定位的状态，并返回旧位值
func execSetBit(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR bit offset is not an integer or out of range")
	}
	valStr := string(args[2])
	var v byte
	// bitmap 只有 0 和 1 两种状态
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return protocol.MakeErrReply("ERR bit is not an integer or out of range")
	}
	// 获取 val 值
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	// 转化为 bitmap
	bitmaps := bitmap.FromBytes(bytes)
	former := bitmaps.GetBit(offset)
	bitmaps.SetBit(offset, v)
	db.PutEntity(key, &database.DataEntity{Data: bitmaps.ToBytes()})
	return protocol.MakeIntReply(int64(former))
}

// 获取存储在字符串键中的特定位的状态
func execGetBit(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR bit offset is not an integer or out of range")
	}
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		return protocol.MakeIntReply(0)
	}
	bitmaps := bitmap.FromBytes(bytes)
	return protocol.MakeIntReply(int64(bitmaps.GetBit(offset)))
}

/*
对某个 bitmap 类型键 的指定范围内的 1 bit 数量进行统计 ，并返回结果，和
bit 模式：范围参数按字节索引计算
byte 模式：范围参数按比特位索引计算
*/
func execBitCount(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	// 获取 value
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		return protocol.MakeIntReply(0)
	}

	byteMode := true
	if len(args) > 3 {
		mode := strings.ToLower(string(args[3]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
			byteMode = true
		} else {
			return protocol.MakeErrReply("ERR syntax error")
		}
	}
	var size int64
	bitmaps := bitmap.FromBytes(bytes)
	// byte 形式
	if byteMode {
		size = int64(len(*bitmaps))
		// bit 形式
	} else {
		size = int64(bitmaps.Bitsize())
	}

	// BITCOUNT key [start end]
	var begin, end int
	if len(args) > 1 {
		var err2 error
		var startIndex, endIndex int64
		// 解析 start 和 end
		startIndex, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		endIndex, err2 = strconv.ParseInt(string(args[3]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		begin, end = utils.ConvertRange(startIndex, endIndex, size)
		if begin < 0 {
			return protocol.MakeIntReply(0)
		}
	}

	var count int64
	if byteMode {
		bitmaps.ForEachByte(begin, end, func(offset int64, val byte) bool {
			// bits.OnesCount8() 统计单个字节中有多少个 1
			count += int64(bits.OnesCount8(val))
			return true
		})
	} else {
		bitmaps.ForEachBit(int64(begin), int64(end), func(offset int64, val byte) bool {
			if val > 0 {
				count++
			}
			return true
		})
	}
	return protocol.MakeIntReply(count)
}

// 查找某个 bitmap 键中 ，指定范围内第一个出现 0 或 1 的 bit 位的位置索引
func execBitPos(db *DB, args [][]byte) myredis.Reply {
	// BITPOS key bit [start] [end] [BIT|BYTE]

	key := string(args[0])
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		return protocol.MakeIntReply(-1)
	}
	valStr := string(args[2])
	// 待查找的 0 / 1
	var v byte
	if valStr == "1" {
		v = 1
	} else if valStr == "0" {
		v = 0
	} else {
		return protocol.MakeErrReply("ERR bit is not an integer ot out of range")
	}

	byteMode := true
	if len(args) > 4 {
		mode := strings.ToLower(string(args[4]))
		if mode == "bit" {
			byteMode = false
		} else if mode == "byte" {
			byteMode = true
		} else {
			return protocol.MakeErrReply("ERR syntax error")
		}
	}
	var size int64
	bitmaps := bitmap.FromBytes(bytes)
	if byteMode {
		size = int64(len(*bitmaps))
	} else {
		size = int64(bitmaps.Bitsize())
	}
	var begin, end int
	if len(args) > 2 {
		var err2 error
		var startIndex, endIndex int64

		startIndex, err2 = strconv.ParseInt(string(args[2]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		endIndex, err2 = strconv.ParseInt(string(args[3]), 10, 64)
		if err2 != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		begin, end = utils.ConvertRange(startIndex, endIndex, size)
		if begin < 0 {
			return protocol.MakeIntReply(0)
		}
	}
	if byteMode {
		begin *= 8
		end *= 8
	}
	var offset = int64(-1)
	// 获取指定范围内第一个出现 0 或 1 的 bit 位的位置索引
	bitmaps.ForEachBit(int64(begin), int64(end), func(o int64, val byte) bool {
		if v == val {
			offset = o
			return false
		}
		return true
	})
	return protocol.MakeIntReply(offset)
}

// 获取一个随机的 key
func getRandomKey(db *DB, args [][]byte) myredis.Reply {
	key := db.data.RandomKeys(1)
	if len(key) == 0 {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply([]byte(key[0]))
}

func init() {
	registerCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("SetNX", execSetNX, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("SetEX", execSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("PSetEX", execPSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("MSet", execMSet, prepareMSet, undoMSet, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 2)
	registerCommand("MSetNX", execMSetNX, prepareMSet, undoMSet, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)

	registerCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("MGet", execMGet, prepareMGet, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("GetEX", execGetEX, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("GetSet", execGetSet, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("GetDel", execGetDel, writeFirstKey, rollbackFirstKey, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)

	registerCommand("Incr", execIncr, writeFirstKey, rollbackFirstKey, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("IncrBy", execIncrBy, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("IncrByFloat", execIncrByFloat, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("Decr", execDecr, writeFirstKey, rollbackFirstKey, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("DecrBy", execDecrBy, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)

	registerCommand("StrLen", execStrLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Append", execAppend, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("SetRange", execSetRange, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("GetRange", execGetRange, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)

	registerCommand("GetBit", execGetBit, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("SetBit", execSetBit, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BitCount", execBitCount, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("BitPos", execBitPos, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)

	registerCommand("Randomkey", getRandomKey, readAllKeys, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
}
