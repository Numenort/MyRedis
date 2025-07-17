package database

import (
	"myredis/datastruct/dict"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/timewheel"
	"myredis/protocol"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

type CmdLine = [][]byte

type DB struct {
	index int

	data *dict.ConcurrentDict

	ttlMap *dict.ConcurrentDict

	versionMap *dict.ConcurrentDict

	addAof func(CmdLine)

	deleteCallback database.KeyEventCallback

	insertCallback database.KeyEventCallback
}

// 执行命令的接口
type ExecFunc func(db *DB, args [][]byte) myredis.Reply

// PreFunc 在将命令加入 `multi` 队列时分析命令行
// 返回相关的写键（write keys）和读键（read keys）
type PreFunc func(args [][]byte) ([]string, []string)

// UndoFunc 用于生成某个命令的回滚操作指令（undo logs）
// 当事务需要回滚时，将按顺序依次执行这些 undo 操作
type UndoFunc func(db *DB, args [][]byte) []CmdLine

func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

func (db *DB) Exec(c myredis.Connection, cmdLine [][]byte) myredis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 设置连接进入事务状态 (多命令执行)
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
		// 放弃连接
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	// 连接处于事务状态，所有非事务控制命令都会被加入队列而不是立即执行
	if c != nil && c.InMultiState() {
		return EnqueueCmd(c, cmdLine)
	}

	return db.execNormalCommand(cmdLine)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) myredis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	// 写键需要版本信息
	db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	exfun := cmd.executor
	// 使用命令执行函数执行命令
	return exfun(db, cmdLine[1:])
}

func (db *DB) execWithLock(cmdLine [][]byte) myredis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	// 不存在的命令
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	// 命令参数数量错误
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	exfun := cmd.executor
	return exfun(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	// 固定数量参数
	if arity >= 0 {
		return argNum == arity
	}
	// 不定长参数（至少需要xx个参数）
	return argNum >= -arity
}

// ******************** Lock Function ********************
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

// ******************** Data Access ********************

// 获取数据实体，即 val
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	// 获取原始的键值
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// 写入数据实体
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	res := db.data.PutWithLock(key, entity)
	// 如果有插入的回调函数，执行该函数
	if callback := db.insertCallback; callback != nil && res > 0 {
		callback(db.index, key, entity)
	}
	return res
}

// 从内存数据库移除 key (需要同时移除时间轮中的定时清理任务)
func (db *DB) Remove(key string) {
	raw, deleted := db.data.RemoveWithLock(key)
	db.ttlMap.Remove(key)
	// 定时的清理任务的键
	taskKey := genExpireTask(key)
	// 定时清理任务取消
	timewheel.Cancel(taskKey)
	// 如果有删除的回调函数，那么就执行对应的回调函数
	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// 批量移除 key，返回移除的 key 数量
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	// 检查内存数据库中是否存在对应的 key
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// 清空整个数据库
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}

// ******************** TTL Functions ********************

// 生成用于过期处理的任务 key
func genExpireTask(key string) string {
	return "expire:" + key
}

// 对 key 设置 ttl，将过期任务加入时间轮
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	// 将过期任务加入时间轮
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)

		logger.Info("expire " + key)
		// 检查当前 TTL 是否仍然有效
		rawExpiredTime, ok := db.ttlMap.Get(key)
		// key 已经被删除或更新过
		if !ok {
			return
		}
		expireTime, _ := rawExpiredTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

// 移除过期时间
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	// 时间轮中移除对应的过期 key 操作
	timewheel.Cancel(taskKey)
}

// 检查 key 是否过期
func (db *DB) IsExpired(key string) bool {
	// 从 ttlMap 得到过期时间
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

// ******************** Add Version Info ********************

// 获取版本信息
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

// 对 key 加入版本信息
func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode)
	}
}
