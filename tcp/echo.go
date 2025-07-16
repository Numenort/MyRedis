package tcp

import (
	"bufio"
	"context"
	"io"
	"myredis/lib/logger"
	"myredis/lib/sync/atomic"
	"myredis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

func (e *EchoClient) Close() error {
	e.Waiting.WaitWithTimeout(10 * time.Second)
	e.Conn.Close()
	return nil
}

// echo 服务的 Handle函数
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	// 新建一个连接，存储在map里
	client := &EchoClient{
		Conn: conn,
	}
	// sync.Map不允许复制，需要指针
	h.activeConn.Store(client, struct{}{}) // struct{}{} 是 struct{}类型的值

	reader := bufio.NewReader(conn)
	for {
		// may occurs: client EOF, client timeout, server early close
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 写回（echo）
		client.Waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

func (h *EchoHandler) Close() error {
	logger.Info("handler shuting down")
	h.closing.Set(true)
	// 遍历活跃连接，挨个关闭连接
	h.activeConn.Range(
		func(key interface{}, val interface{}) bool {
			client := key.(*EchoClient)
			_ = client.Close()
			return true
		})
	return nil
}
