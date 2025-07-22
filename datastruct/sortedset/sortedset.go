package sortedset

import "strconv"

type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

// 添加节点，如果成员之前存在，更新分数，返回 false；新成员返回 true
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	// 获取原先的成员
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	// 如果之前存在
	if ok {
		// 更新跳表节点
		if score != element.Score {
			sortedSet.skiplist.remove(member, element.Score)
			sortedSet.skiplist.insert(member, score)
		}
		return false
	}
	sortedSet.skiplist.insert(member, score)
	return true
}

// 从集合中删除成员
func (sortedSet *SortedSet) Remove(member string) bool {
	val, ok := sortedSet.dict[member]
	if ok {
		sortedSet.skiplist.remove(member, val.Score)
		delete(sortedSet.dict, member)
		return true
	}
	return false
}

func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

// 如果 rank 为 -1，查找失败
func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := sortedSet.dict[member]
	if !ok {
		return -1
	}
	rank = sortedSet.skiplist.getRank(member, element.Score)
	if desc {
		rank = sortedSet.skiplist.length - rank
	} else {
		// 由于 skiplist 查找 rank 为 0 时，未找到 member
		rank--
	}
	return rank
}

// 对 min 和 max 边界的每一个元素进行遍历，支持 offset 以及 limit (limit < 0 代表没有限制)
func (sortedSet *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	var node *node
	if desc {
		node = sortedSet.skiplist.getLastInRange(min, max)
	} else {
		node = sortedSet.skiplist.getFirstInRange(min, max)
	}

	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		isMin := min.less(&node.Element)
		isMax := max.greater(&node.Element)
		if !isMin || !isMax {
			break
		}
	}
}

// 对于 start 和 end 范围内的每一个元素进行 consumer 函数操作
func (sortedSet *SortedSet) ForEachByRank(start int64, end int64, desc bool, consumer func(element *Element) bool) {
	size := sortedSet.Len()
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if end < start || end > size {
		panic("illegal end " + strconv.FormatInt(end, 10))
	}

	var node *node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(size - start)
		}
	} else {
		// start = 0: 即第一个节点
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(start + 1)
		}
	}

	iterSize := int(end - start)
	for i := 0; i < iterSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

// 按排名返回范围内的元素
func (sortedSet *SortedSet) RangeByRank(start int64, end int64, desc bool) []*Element {
	sliceSize := int(end - start)
	slice := make([]*Element, sliceSize)
	i := 0
	sortedSet.ForEachByRank(start, end, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

// 统计在范围内的集合成员数量
func (sortedSet *SortedSet) RangeCount(min Border, max Border) int64 {
	var i int64 = 0
	sortedSet.ForEachByRank(0, sortedSet.Len(), false, func(element *Element) bool {
		// 元素大于 min
		isMin := min.less(element)
		// 当前元素小于 min，继续遍历
		if !isMin {
			return true
		}
		// 元素小于 max
		isMax := max.greater(element)
		// 当前元素大于 max，停止遍历
		if !isMax {
			return false
		}
		i++
		return true
	})
	return i
}

// 按 Border 返回范围内的元素
func (sortedSet *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	sortedSet.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

func (sortedSet *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := sortedSet.skiplist.RemoveRange(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

func (sortedSet *SortedSet) PopMin(count int) []*Element {
	firstNode := sortedSet.skiplist.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if firstNode == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   firstNode.Score,
		Exclude: false,
	}
	removed := sortedSet.skiplist.RemoveRange(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return removed
}

func (sortedSet *SortedSet) 
