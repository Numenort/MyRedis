package dict

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Len() int
	Remove(key string) (val interface{}, result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
	DictScan(cursor int, count int, pattern string) ([][]byte, int)
}
