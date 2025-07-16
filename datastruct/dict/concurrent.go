package dict

import (
	"math"
	"math/rand"
	"myredis/lib/wildcard"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ConcurrentDict struct {
	table      []*shard // 键值对会被哈希分散到这些分片中
	count      int32    //字典中键值对的总数量
	shardCount int      // 分片数量
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

// 计算一个大于等于 param 的最小的2的幂次方作为分片数量
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	if shardCount == 1 {
		table := []*shard{
			{
				m: make(map[string]interface{}),
			},
		}
		return &ConcurrentDict{
			table:      table,
			count:      0,
			shardCount: shardCount,
		}
	}
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: shardCount,
	}
	return d
}

const prime32 = uint32(16777619)

// FNV-1a 32位哈希算法
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 计算 key 需要存储在哪个hash分片
func (dict *ConcurrentDict) spread(key string) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	// 只有一个分片
	if len(dict.table) == 1 {
		return 0
	}
	hashCode := fnv32(key)
	// hashSize - 1 除了开头的 0，全为 1，因此直接 & 获得分片索引
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & hashCode
}

func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	val, exists = s.m[key]
	return val, exists
}

func (dict *ConcurrentDict) GetWithLock(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	// 获取 key 对应的分片
	index := dict.spread(key)
	s := dict.getShard(index)
	val, exists = s.m[key]
	return val, exists
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	// 保证原子性
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// update 键值
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

// 用于已持有锁的内部调用
func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

// 只有 key 不存在时，该函数才会更新值
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// 只有 key 存在时，才会更新值
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return nil, 0
}

func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// for each key, value do consumer function
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.RLock()
		f := func() bool {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	// 使用 map 去重
	result := make(map[string]struct{})
	Rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(Rand.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

// 提取 key 对应的所有 shard index
func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		index := dict.spread(key)
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// 对涉及的所有 key 统一加锁
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	// 获取 keys 的所有 shard index
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wkey := range writeKeys {
		idx := dict.spread(wkey)
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		// 写锁
		if w {
			mu.Lock()
			// 读锁
		} else {
			mu.RLock()
		}
	}
}

func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wkey := range writeKeys {
		idx := dict.spread(wkey)
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

func (dict *ConcurrentDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	size := dict.Len()
	result := make([][]byte, 0)

	if pattern == "*" && count >= size {
		return stringsToBytes(dict.Keys()), 0
	}

	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}

	shardCount := len(dict.table)
	shardIndex := cursor

	for shardIndex < shardCount {
		shard := dict.table[shardIndex]
		shard.mutex.RLock()
		if len(result)+len(shard.m) > count && shardIndex > cursor {
			shard.mutex.RUnlock()
			return result, shardIndex
		}

		for key := range shard.m {
			if pattern == "*" || matchKey.IsMatch(key) {
				result = append(result, []byte(key))
			}
		}
		shard.mutex.RUnlock()
		shardIndex++
	}
	return result, 0
}

func stringsToBytes(strSlice []string) [][]byte {
	byteSlice := make([][]byte, 0)
	for i, str := range strSlice {
		byteSlice[i] = []byte(str)
	}
	return byteSlice
}
