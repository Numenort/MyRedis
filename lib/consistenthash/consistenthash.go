package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

type hashFunc func(data []byte) uint32

type Map struct {
	hashFunc hashFunc
	replicas int   // 每个真实节点对应的虚拟节点数量
	keys     []int // 所有虚拟节点的哈希值列表
	hashMap  map[int]string
}

func New(replicas int, fn hashFunc) *Map {
	m := &Map{
		hashFunc: fn,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// 向哈希环添加一个或多个真实节点
func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// 从 key 中提取分区键
func getPartitionKey(key string) string {
	begin := strings.Index(key, "{")
	if begin == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == begin+1 {
		return key
	}
	return key[begin+1 : end]
}

// 根据 key 选择对应的真实节点：
// 计算 key 的哈希值，在环上顺时针找到第一个大于等于该哈希的虚拟节点
func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	partitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(partitionKey)))

	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
