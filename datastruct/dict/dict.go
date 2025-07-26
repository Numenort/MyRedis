package dict

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) (result int)
	// 如果键不存在则插入键值对
	PutIfAbsent(key string, val interface{}) (result int)
	// 如果键存在则更新值
	PutIfExists(key string, val interface{}) (result int)
	Len() int
	Remove(key string) (val interface{}, result int)
	ForEach(consumer Consumer)
	Keys() []string
	// 随机获取指定数量的可能重复键
	RandomKeys(limit int) []string
	// 随机获取指定数量的不重复键
	RandomDistinctKeys(limit int) []string
	Clear()
	DictScan(cursor int, count int, pattern string) ([][]byte, int)
}
