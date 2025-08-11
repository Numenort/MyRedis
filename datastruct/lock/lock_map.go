package lock

import (
	"sort"
	"sync"
)

const (
	prime32 = uint32(16777619)
)

// 分片锁
type Locks struct {
	table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table: table,
	}
}

// 计算字符串 key 的 32 位哈希值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 将哈希值映射到锁分片数组的有效索引
func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & hashCode
}

// 哈希，分片后锁上对应的锁
func (locks Locks) Lock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

func (locks Locks) RLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RLock()
}

func (locks Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

func (locks Locks) RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}

// 将一组 key 转换为唯一的锁分片索引
func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	// 去重
	for _, key := range keys {
		index := locks.spread(fnv32(key))
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] > indices[j]
		}
		return indices[i] < indices[j]
	})
	return indices
}

func (locks Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Lock()
	}
}

func (locks Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RLock()
	}
}

func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Unlock()
	}
}

func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RUnlock()
	}
}

func (locks Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		index := locks.spread(fnv32(wKey))
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, exists := writeIndexSet[index]
		mu := locks.table[index]
		if exists {
			// 写键
			mu.Lock()
		} else {
			// 读键
			mu.RLock()
		}
	}
}

func (locks Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	// 按顺序释放锁
	indices := locks.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		index := locks.spread(fnv32(wKey))
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, exists := writeIndexSet[index]
		mu := locks.table[index]
		if exists {
			// 写键
			mu.Unlock()
		} else {
			// 读键
			mu.RUnlock()
		}
	}
}
