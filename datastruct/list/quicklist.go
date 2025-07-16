package list

import "container/list"

const pagesize = 1024

type QuickList struct {
	data *list.List
	size int
}

type iterator struct {
	node   *list.Element
	offset int
	ql     *QuickList
}

func NewQuickList() *QuickList {
	return &QuickList{
		data: list.New(),
	}
}

func (ql *QuickList) Add(val interface{}) {
	ql.size++
	if ql.data.Len() == 0 {
		page := make([]interface{}, 0, pagesize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	backNode := ql.data.Back()
	backPage := backNode.Value.([]interface{})
	// capacity to the upper limit
	if len(backPage) == cap(backPage) {
		page := make([]interface{}, 0, pagesize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	backPage = append(backPage, val)
	backNode.Value = backPage
}

func (ql *QuickList) find(index int) *iterator {
	if ql == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}
	var n *list.Element
	var page []interface{}
	var pageBeg int
	if index < ql.size/2 {
		n = ql.data.Front()
		pageBeg = 0
		for {
			page = n.Value.([]interface{})
			// find the page of index
			if pageBeg+len(page) > index {
				break
			}
			pageBeg += len(page)
			n = n.Next()
		}
	} else {
		n = ql.data.Back()
		pageBeg = ql.size
		for {
			page = n.Value.([]interface{})
			pageBeg -= len(page)
			if pageBeg <= index {
				break
			}
			n = n.Prev()
		}
	}
	pageOffset := index - pageBeg
	return &iterator{
		node:   n,
		offset: pageOffset,
		ql:     ql,
	}
}

func (iter *iterator) get() interface{} {
	return iter.page()[iter.offset]
}

func (iter *iterator) page() []interface{} {
	return iter.node.Value.([]interface{})
}

func (iter *iterator) next() bool {
	page := iter.node.Value.([]interface{})
	if iter.offset < len(page)-1 {
		iter.offset++
		return true
	}
	if iter.node == iter.ql.data.Back() {
		iter.offset = len(page)
		return false
	}
	iter.offset = 0
	iter.node = iter.node.Next()
	return true
}

func (iter *iterator) prev() bool {
	if iter.offset > 0 {
		iter.offset--
		return true
	}
	// already at first page
	if iter.node == iter.ql.data.Front() {
		iter.offset = -1
		return false
	}
	iter.node = iter.node.Prev()
	prevPage := iter.node.Value.([]interface{})
	iter.offset = len(prevPage) - 1
	return true
}

func (iter *iterator) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}
	page := iter.page()
	return iter.offset == len(page)
}

func (iter *iterator) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}

func (iter *iterator) set(val interface{}) {
	page := iter.page()
	page[iter.offset] = val
}

func (ql *QuickList) Get(index int) (val interface{}) {
	iter := ql.find(index)
	return iter.get()
}

func (ql *QuickList) Set(index int, val interface{}) {
	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList) Insert(index int, val interface{}) {
	if index == ql.size {
		ql.Add(val)
		return
	}
	iter := ql.find(index)
	page := iter.node.Value.([]interface{})
	if len(page) < pagesize {
		// insert into not full page
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++
		return
	}
	// 如果当前页已满，需要进行页分裂
	var nextPage []interface{}
	// nextPage 保留原页得前半部分
	nextPage = append(nextPage, page[pagesize/2:]...)
	// 当前页只保留原页的前半部分
	page = page[:pagesize/2]
	if iter.offset < len(page) {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
	} else {
		i := iter.offset - pagesize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...)
		nextPage[i] = val
	}
	// store current page and next page
	iter.node.Value = page
	ql.data.InsertAfter(nextPage, iter.node)
	ql.size++
}

func (iter *iterator) remove() interface{} {
	page := iter.page()
	val := page[iter.offset]
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	if len(page) > 0 {
		// page 非空
		iter.node.Value = page
		// 如果移除的是页的最后一个元素，迭代器需要后移
		if iter.offset == len(page) {
			// 如果不是最后一个页，移动到下一个页的起始位置
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
		}
		// 如果 page 为空
	} else {
		// 移除最后一个页
		if iter.node == iter.ql.data.Back() {
			if prevNode := iter.node.Prev(); prevNode != nil {
				iter.ql.data.Remove(iter.node)
				iter.node = nil
				iter.offset = 0
			}
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}
	iter.ql.size--
	return val
}

func (ql *QuickList) Remove(index int) interface{} {
	iter := ql.find(index)
	return iter.remove()
}

func (ql *QuickList) Len() int {
	return ql.size
}

func (ql *QuickList) RemoveLast() interface{} {
	if ql.Len() == 0 {
		return nil
	}
	ql.size--
	lastNode := ql.data.Back()
	lastPage := lastNode.Value.([]interface{})
	// 如果最后一个页只剩一个元素
	if len(lastPage) == 1 {
		// 移除整个链表节点
		ql.data.Remove(lastNode)
		return lastPage[0]
	}
	// 移除页对应元素
	val := lastPage[len(lastPage)-1]
	lastPage = lastPage[:len(lastPage)-1]
	lastNode.Value = lastPage
	return val
}

func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		// 这里是因为 remove 函数会自动移动结点到下一个位置
		iter.prev()
	}
	return removed
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}
	return removed
}

func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		panic("list is nil")
	}
	if ql.Len() == 0 {
		return
	}
	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	if ql == nil {
		panic("list is nil")
	}
	if ql.Len() == 0 {
		return false
	}
	contains := false
	ql.ForEach(func(i int, actual interface{}) bool {
		if expected(actual) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}
	sliceSize := stop - start
	slice := make([]interface{}, 0, sliceSize)
	iter := ql.find(start)
	i := 0
	for i < sliceSize {
		slice = append(slice, iter.get())
		iter.next()
		i++
	}
	return slice
}

var _ = (List)(&QuickList{})
