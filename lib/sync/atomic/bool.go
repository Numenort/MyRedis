package atomic

import "sync/atomic"

// 线程安全的布尔变量

// Go 中的方法本质上是绑定到类型的普通函数\
// 接收者（如 b *Boolean）只是函数的第一个隐式参数。

type Boolean uint32

// 获取变量 “是/否”
func (b *Boolean) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

func (b *Boolean) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}
