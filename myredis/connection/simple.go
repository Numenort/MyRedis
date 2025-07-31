// 一个简单的假连接，用于测试
package connection

import (
	"fmt"
	"io"
	"myredis/lib/logger"
	"sync"
)

type SimpleConn struct {
	Connection
	buf     []byte
	offset  int
	closed  bool
	waiting chan struct{}

	mu sync.Mutex
}

func NewSimpleConn() *SimpleConn {
	c := &SimpleConn{}
	return c
}

func (c *SimpleConn) Write(b []byte) (int, error) {
	if c.closed {
		return 0, io.EOF
	}
	// 保证并发安全，使用互斥锁
	c.mu.Lock()
	c.buf = append(c.buf, b...)
	c.mu.Unlock()
	// 通知读者可以有新数据可以读了
	c.notify()

	return len(b), nil
}

func (c *SimpleConn) Read(p []byte) (int, error) {
	// 尝试读取数据
	c.mu.Lock()
	n := copy(p, c.buf[c.offset:])
	c.offset += n
	offset := c.offset
	c.mu.Unlock()
	// 处理无数据可读情况
	if n == 0 {
		// 连接关闭
		if c.closed {
			return n, io.EOF
		}
		// 阻塞等待写入的数据流
		c.wait(offset)
		if c.closed {
			return n, io.EOF
		}
		n = copy(p, c.buf[c.offset:])
		c.offset += n
		return n, nil
	}
	if c.closed {
		return n, io.EOF
	}
	return n, nil
}

func (c *SimpleConn) Close() error {
	c.closed = true
	// 通知所有协程连接已关闭
	c.notify()
	return nil
}

// 用于写协程通知读协程有新数据可读或者关闭连接
func (c *SimpleConn) notify() {
	// 如果有读进程正在等待
	if c.waiting != nil {
		// 确保并发下的资源访问安全
		c.mu.Lock()
		if c.waiting != nil {
			logger.Debug(fmt.Sprintf("notify %p", &c.waiting))
			close(c.waiting)
			c.waiting = nil
		}
		c.mu.Unlock()
	}
}

// 用于读协程等待写协程写入数据
func (c *SimpleConn) wait(offset int) {
	// 并发访问 offset
	c.mu.Lock()
	// 如果有新的数据，不等了
	if c.offset != offset {
		return
	}
	if c.waiting == nil {
		c.waiting = make(chan struct{})
	}
	// 确保后续的阻塞操作引用的是锁保护时确定的通道
	waiting := c.waiting
	logger.Debug(fmt.Sprintf("wait on %p", waiting))
	c.mu.Unlock()
	<-waiting
	logger.Debug(fmt.Sprintf("waiting %p finish", waiting))
}

// 清空 Buffer，此时无数据可读
func (c *SimpleConn) Clean() {
	c.waiting = make(chan struct{})
	c.buf = nil
	c.offset = 0
}

func (c *SimpleConn) Bytes() []byte {
	return c.buf
}

func (c *SimpleConn) RemoteAddr() string {
	return ""
}
