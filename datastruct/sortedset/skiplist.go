package sortedset

import (
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 16
)

// 元素为 key-score 对
type Element struct {
	Member string
	Score  float64
}

/*
span:从当前节点（不包括当前节点）沿着某一层（例如 Level i）的 forward 指针到达其下一个节点
（包括下一个节点）时，中间总共跨越了多少个 第 0 层 的节点。

forward:下一个节点
*/
type Level struct {
	forward *node
	span    int64
}

// backward 指向当前节点在 level 0 的前一个节点
type node struct {
	Element
	backward *node
	level    []*Level
}

// header 为哨兵节点，tail 为真实节点
type skiplist struct {
	header *node
	tail   *node
	length int64
	level  int16
}

// Node  LEVEL[0] LEVEL[1] LEVEL[2] LEVEL[3]
//  H		A		 B		  C		  T
//  A       B       nil		 nil     nil
//  B       C        C       nil     nil
//  C	    T		 T        T		  T

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Member: member,
			Score:  score,
		},
		level: make([]*Level, level),
	}

	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

// 获取随机层级，按照二进制位数，取得低位的概率远大于高位
func randomLevel() int16 {
	// 最大 Level
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

func (skiplist *skiplist) insert(member string, score float64) *node {
	/*
		存储在从最高层到最低层（第0层）的遍历过程中，
		每个层级上新节点应该插入到哪个节点之后
		update[i] 将指向在第 i 层上，新节点的前一个节点
	*/
	update := make([]*node, maxLevel)
	/*
		记录在遍历跳表时，从跳表头节点 (skiplist.header) 开始
		到 update[i] 所指向的节点为止，一共跨越了多少个第0层的节点
		rank[i] 是 update[i] 在第0层上的“排名”
	*/
	rank := make([]int64, maxLevel)

	// 寻找插入位置
	node := skiplist.header
	// 从跳表的最高层 (skiplist.level - 1) 开始向下遍历
	for i := skiplist.level - 1; i >= 0; i-- {
		if i == skiplist.level-1 {
			rank[i] = 0
		} else {
			// 如果不是最高层: 继承上一层（更高层）的 rank 值
			rank[i] = rank[i+1]
		}
		// 如果存在其他级别的节点
		if node.level[i] != nil {
			for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				// 下一个节点
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	level := randomLevel()
	// 扩展层级
	if level > skiplist.level {
		for i := skiplist.level; i < level; i++ {
			// 新层级 rank 设置为 0（已有一个节点）
			rank[i] = 0
			// 新层级直连头节点
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// 新建节点，插入跳表
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		// 新节点的下一个是 update[i] 的下一个节点
		node.level[i].forward = update[i].level[i].forward
		// 原始节点的下一个节点是新节点
		update[i].level[i].forward = node
		// rank[0] - rank[i]：“在第 i 层的‘前一站’(update[i])” 和 “在第 0 层的‘前一站’(update[0])” 之间，相差了多少个普通站
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == skiplist.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skiplist.tail = node
	}
	skiplist.length++
	return node
}

func (skiplist *skiplist) removeNode(node *node, update []*node) {
	// update[i] 记录的是在第 i 层上，值小于等于（或字典序小于）目标删除元素，且离目标删除元素最近的那个节点
	// 在各层的最接近节点上更新 span
	for i := int16(0); i < skiplist.level; i++ {
		// 如果 update[i] 的 forward 指针指向了要删除的 node
		if update[i].level[i].forward == node {
			// 更新 update[i] 的 span 值和下一个节点
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].forward != nil {
		// 让被删除节点的下一个节点的 backward 指针指向被删除节点的 backward (即它的新前驱)
		node.level[0].forward.backward = node.backward
	} else {
		// 如果被删除的节点是跳表的最后一个节点
		skiplist.tail = node.backward
	}
	// 从最高层向下检查，如果某一层的 header 的 forward 指针是 nil (即该层已经没有实际节点)，
	// 那么就可以降低跳表的最高层数，以节省内存和保持紧凑。
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	skiplist.length--
}

func (skiplist *skiplist) remove(member string, score float64) bool {
	/*
	 * find backward node (of target) or last node of each level
	 * their forward need to be updated
	 */
	update := make([]*node, maxLevel)
	node := skiplist.header
	// 寻找待删除节点
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	// 利用先前的函数实现删除操作
	if node != nil && score == node.Score && node.Member == member {
		skiplist.removeNode(node, update)
		// free x
		return true
	}
	return false
}

// 获取 member 的 rank，返回 0 代表未找到 member
func (skiplist *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	h := skiplist.header
	// 从最高层开始查找，直到最底层
	for i := skiplist.level - 1; i >= 0; i-- {
		for h.level[i].forward != nil && (h.level[i].forward.Score < score ||
			(h.level[i].forward.Member <= member &&
				h.level[i].forward.Score == score)) {
			rank += h.level[i].span
			h = h.level[i].forward
		}
		if h.Member == member {
			return rank
		}
	}
	return 0
}

// 由 rank 得到对应的 node
func (skiplist *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	h := skiplist.header

	for level := skiplist.level - 1; level >= 0; level-- {
		for h.level[level].forward != nil && (i+h.level[level].span) <= rank {
			i += h.level[level].span
			h = h.level[level].forward
		}
		if i == rank {
			return h
		}
	}
	return nil
}

// 判断 skiplist 是否存在落在某个区间范围内的节点
func (skiplist *skiplist) hasInRange(min Border, max Border) bool {
	// min 和 max 存在交集
	if min.isIntersected(max) {
		return false
	}
	// min > tail
	node := skiplist.tail
	if node == nil || !min.less(&node.Element) {
		return false
	}
	// max < head
	node = skiplist.header
	if node == nil || !max.greater(&node.Element) {
		return false
	}
	return true
}

// 得到第一个在指定范围内的节点（level 0）
func (skiplist *skiplist) getFirstInRange(min Border, max Border) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	node := skiplist.header
	// 层级由高到低扫描，找到最后一个 不大于 min 的节点
	for level := skiplist.level - 1; level >= 0; level-- {
		// 下一个节点不为空，且下一个节点值小于 min 时
		for node.level[level].forward != nil && !min.less(&node.level[level].forward.Element) {
			node = node.level[level].forward
		}
	}
	// node 为第一个大于 min 的节点
	node = node.level[0].forward
	// 如果 node 大于 max 即不在范围
	if !max.greater(&node.Element) {
		return nil
	}
	return node
}

// 得到最后一个在指定范围内的节点（level 0）
func (skiplist *skiplist) getLastInRange(min Border, max Border) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}
	node := skiplist.header
	// 层级由高到低扫描，找到第一个 不小于 max 的节点
	for level := skiplist.level - 1; level >= 0; level-- {
		// 下一个节点不为空，且下一个节点值小于 max 时
		for node.level[level].forward != nil && max.greater(&node.level[level].forward.Element) {
			node = node.level[level].forward
		}
	}
	if !min.less(&node.Element) {
		return nil
	}
	return node
}

// 按范围移除节点，limit 小于等于 0 代表没有限制
func (skiplist *skiplist) RemoveRange(min Border, max Border, limit int) (removed []*Element) {
	// 存储每个层级的起始节点位置
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	node := skiplist.header
	//
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			// 如果 min 小于 节点值，已经找到起点
			if min.less(&node.level[i].forward.Element) {
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	node = node.level[0].forward
	// 从 level 0 开始删除
	for node != nil {

		if !max.greater(&node.Element) {
			break
		}
		nextNode := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = nextNode
	}
	return removed
}

// 按照排名移除节点
func (skiplist *skiplist) RemoveRangeByRank(start int64, end int64) (removed []*Element) {
	var i int64 = 0
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	// 填充 update，找到每一层最接近的 node
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		// 找到小于 start 的最大节点
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	// 在范围内的起始节点，i 代表 level 0 层级的位次
	node = node.level[0].forward
	for node != nil && i < end {
		nextNode := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = nextNode
		i++
	}
	return removed
}
