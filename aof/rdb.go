package aof

import (
	"myredis/config"
	"myredis/lib/logger"
	"os"
	"strconv"
	"time"

	"myredis/datastruct/dict"
	List "myredis/datastruct/list"
	"myredis/datastruct/set"
	"myredis/datastruct/sortedset"
	"myredis/interface/database"

	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
)

/*
从AOF文件生成RDB快照文件

参数rdbFilename: 输出的RDB文件路径

阻塞执行，完成后原子替换目标文件
*/
func (persister *Persister) GenerateRDB(rdbFileName string) error {
	// 获取上下文，准备生成 RDB
	RewriteCtx, err := persister.prepareGenerateRDB(nil, nil)
	if err != nil {
		return err
	}

	// RDB 生成结束
	err = persister.generateRDB(RewriteCtx)
	if err != nil {
		return err
	}

	err = RewriteCtx.tempFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(RewriteCtx.tempFile.Name(), rdbFileName)
	if err != nil {
		return err
	}
	return nil
}

/*
为副本同步异步生成RDB文件

参数rdbFilename: 输出文件路径

参数listener: 接收后续增量数据的监听器

参数hook: AOF暂停期间执行的回调函数

生成完成后自动替换文件并触发监听器
*/
func (persister *Persister) GenerateRDBForReplication(rdbFileName string, listener Listener, hook func()) error {
	// 获取上下文，准备生成 RDB
	RewriteCtx, err := persister.prepareGenerateRDB(listener, hook)
	if err != nil {
		return err
	}

	// RDB 生成结束
	err = persister.generateRDB(RewriteCtx)
	if err != nil {
		return err
	}

	err = RewriteCtx.tempFile.Close()
	if err != nil {
		return err
	}
	return nil
}

/*
RDB生成前的准备工作

暂停AOF写入、同步磁盘、创建临时文件

返回重写上下文，支持监听器和钩子函数
*/
func (persister *Persister) prepareGenerateRDB(newListener Listener, hook func()) (*RewriteContext, error) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 确保 AOF 文件已被存储
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// 获取当前 AOF 文件的大小，用于重写
	fileInfo, _ := os.Stat(persister.aofFilename)
	fileSize := fileInfo.Size()

	tempFile, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("temp file create failed")
		return nil, err
	}

	if newListener != nil {
		persister.listeners[newListener] = struct{}{}
	}
	// 执行钩子函数
	if hook != nil {
		hook()
	}
	return &RewriteContext{
		tempFile: tempFile,
		fileSize: fileSize,
	}, nil
}

/*
执行RDB文件内容生成

遍历内存数据，按RDB格式编码写入临时文件

支持字符串、列表、集合、哈希、有序集合等数据类型

包含版本信息、过期时间等元数据
*/
func (persister *Persister) generateRDB(ctx *RewriteContext) error {
	// 加载 AOF 文件，获取数据库状态
	tempHandler := persister.newRewriteHandler()
	tempHandler.LoadAof(int(ctx.fileSize))

	encoder := rdb.NewEncoder(ctx.tempFile).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}
	// aof-preamble：RDB前导机制，将 RDB 快照数据作为 AOF 文件的开头部分
	auxMap := map[string]string{
		"myredis-version": "0.0.1",
		"redis-bits":      "64",
		"aof-preamble":    "0",
		"ctime":           strconv.FormatInt(time.Now().Unix(), 10),
	}

	if config.Properties.AofUseRdbPreamble {
		auxMap["aof-preamble"] = "1"
	}
	// 写入 AUXMAP（Redis服务器元数据信息）
	for key, val := range auxMap {
		err := encoder.WriteAux(key, val)
		if err != nil {
			return err
		}
	}

	// 写入数据库信息
	for i := 0; i < config.Properties.Databases; i++ {
		keyCount, ttlCount := tempHandler.db.GetDBSize(i)
		if keyCount == 0 {
			continue
		}
		// 写入数据库元信息
		err = encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount))
		if err != nil {
			return err
		}
		var err2 error
		// 遍历每个键值对，根据不同的类型使用 RDB 库写入键值对
		tempHandler.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var options []interface{}
			if expiration != nil {
				options = append(options, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			switch object := entity.Data.(type) {
			case []byte:
				// string
				err = encoder.WriteStringObject(key, object, options...)
			case List.List:
				values := make([][]byte, object.Len())
				object.ForEach(func(i int, val interface{}) bool {
					bytes, _ := val.([]byte)
					values = append(values, bytes)
					return true
				})
				err = encoder.WriteListObject(key, values, options...)
			case dict.Dict:
				hashTable := make(map[string][]byte)
				object.ForEach(func(key string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hashTable[key] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hashTable, options)
			case *set.Set:
				values := make([][]byte, object.Len())
				object.ForEach(func(member string) bool {
					values = append(values, []byte(member))
					return true
				})
				err = encoder.WriteSetObject(key, values, options...)
			case *sortedset.SortedSet:
				var entries []*model.ZSetEntry
				object.ForEachByRank(int64(0), int64(object.Len()), true, func(element *sortedset.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, options...)
			}
			if err != nil {
				err2 = err
				return false
			}
			return true
		})
		if err2 != nil {
			return err2
		}
	}
	// 写入结尾
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}
