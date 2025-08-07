/*
Package pool 提供一个可复用对象的资源池，适用于Redis 连接场景。

支持：
- 设置最大空闲数（MaxIdle）和最大活跃数（MaxActive）。
- 获取对象时优先复用空闲对象，超出限制时可阻塞等待。
- 放回对象时若有人等待则直接移交，否则入池或销毁。
- 并发安全，支持多 goroutine 使用。
- 自定义创建（factory）和销毁（finalizer）逻辑。

调用 Close 会关闭池并释放所有空闲资源。
*/
package pool

import (
	"errors"
	"sync"
)

var (
	ErrClosed  = errors.New("pool closed")
	ErrMaxConn = errors.New("reach max connection limit")
)

type PoolConfig struct {
	MaxIdle     int
	MaxActivate int
}

type request chan interface{}

type Pool struct {
	PoolConfig
	factory       func() (interface{}, error) // 创建新对象的工厂函数
	finalizer     func(x interface{})         // 用于销毁对象
	idles         chan interface{}            // 空闲对象队列
	waitingReqs   []request                   // 等待获取对象的请求队列
	activateCount int                         // 当前已创建且正在使用的对象数量
	mu            sync.Mutex
	closed        bool
}

func NewPool(factory func() (interface{}, error), finalizer func(x interface{}), cfg PoolConfig) *Pool {
	return &Pool{
		factory:     factory,
		finalizer:   finalizer,
		idles:       make(chan interface{}, cfg.MaxIdle),
		waitingReqs: make([]request, 0),
		PoolConfig:  cfg,
	}
}

// getOnNoIdle 当空闲池中无对象时，尝试创建新对象或进入等待
func (pool *Pool) getOnNoIdle() (interface{}, error) {
	pool.mu.Lock()
	// 连接已达上限，无法创建新的连接
	if pool.activateCount >= pool.MaxActivate {
		req := make(chan interface{}, 1)
		// 加入 waitingReqs 队列，等待获取对象
		pool.waitingReqs = append(pool.waitingReqs, req)
		pool.mu.Unlock()
		x, ok := <-req
		if !ok {
			return nil, ErrMaxConn
		}
		return x, nil
	}

	// 否则创建新的连接
	pool.activateCount++
	pool.mu.Unlock()
	x, err := pool.factory()
	if err != nil {
		// 创建失败，状态恢复
		pool.mu.Lock()
		pool.activateCount--
		pool.mu.Unlock()
		return nil, err
	}
	return x, nil
}

// 从连接池中获取一个对象
func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	// 池已关闭
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}
	pool.mu.Unlock()

	select {
	// 从空闲队列 idles 中非阻塞获取一个对象
	case item := <-pool.idles:
		return item, nil
	default:
		return pool.getOnNoIdle()
	}
}

// 向连接池中返回一个对象
func (pool *Pool) Put(x interface{}) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		// 连接关闭，销毁对象
		pool.finalizer(x)
		return
	}

	// 优先返回等待请求队列
	if len(pool.waitingReqs) > 0 {
		// 取出第一个等待请求的 channel
		req := pool.waitingReqs[0]
		copy(pool.waitingReqs, pool.waitingReqs[1:])                  // 前移剩余元素
		pool.waitingReqs = pool.waitingReqs[:len(pool.waitingReqs)-1] // 恢复切片长度
		req <- x
		pool.mu.Unlock()
		return
	}
	// 送入空闲队列
	select {
	case pool.idles <- x:
		pool.mu.Unlock()
		return
	default:
		// 销毁该对象，对象数量减少
		pool.mu.Unlock()
		pool.activateCount--
		pool.finalizer(x)
	}
}

func (pool *Pool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	// 关闭连接
	pool.closed = true
	close(pool.idles)

	// 唤醒所有等待中的 Get 请求
	for _, req := range pool.waitingReqs {
		close(req)
	}
	pool.waitingReqs = nil
	pool.mu.Unlock()

	// 销毁所有空闲对象
	for x := range pool.idles {
		pool.finalizer(x)
	}
}
