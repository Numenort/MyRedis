// 算法原理：
//   1. 使用一个固定的纪元（epoch）作为时间基准，生成的时间戳 = 当前时间 - epoch，单位毫秒。
//   2. 将生成的时间戳、节点 ID（nodeID）和同一毫秒内的自增序列（sequence）拼接为 64 位整数：
//        [  时间戳 (42bit)  |  节点 ID (12bit)  |  序列号 (10bit) ]
//   3. 在同一毫秒内，如果序列号达到上限，就自旋等待到下一毫秒。
//
// 整体流程：
//   MakeIDGenerator() 初始化节点，计算基准 epoch；
//   NextID() 获取当前时间戳、判断序列是否需自增或重置、等待下一毫秒（如有必要）、
//           最后拼装成 64 位整型 ID 并返回。

package idgenerator

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

const (
	epoch0        int64 = 1288834974657
	timeLeft      uint8 = 22
	nodeLeft      uint8 = 10
	nodeMask      int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
	maxSequence   int64 = -1 ^ (-1 << uint64(nodeLeft))
	maxBackwardMs int64 = 10
)

type IDGenerator struct {
	mu        *sync.Mutex
	lastStamp int64
	nodeID    int64
	sequence  int64
	epoch     time.Time
}

func MakeIDGenerator(node string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

func (id *IDGenerator) NextID() (int64, error) {
	id.mu.Lock()
	defer id.mu.Unlock()

	timeStamp := time.Since(id.epoch).Nanoseconds() / 1000000
	// 时钟回拨检测
	if timeStamp < id.lastStamp {
		diff := id.lastStamp - timeStamp
		if diff > maxBackwardMs {
			return 0, fmt.Errorf("clock backward too far: %dms", diff)
		}
		// 自旋等待
		for timeStamp < id.lastStamp {
			time.Sleep(time.Millisecond)
			timeStamp = time.Since(id.epoch).Nanoseconds() / 1000000
		}
	}

	// 同一毫秒内：递增序列号
	if id.lastStamp == timeStamp {
		id.sequence = (id.sequence + 1) & maxSequence
		if id.sequence == 0 {
			// 序列号溢出，阻塞到下一毫秒
			for timeStamp <= id.lastStamp {
				timeStamp = time.Since(id.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		// 新毫秒，重置序列号
		id.sequence = rand.Int63() & maxSequence
	}
	id.lastStamp = timeStamp
	// 组装 ID
	generated := (timeStamp << timeLeft) | (id.nodeID << nodeLeft) | id.sequence
	return generated, nil
}
