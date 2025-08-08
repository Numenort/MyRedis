// Client 是一个 Redis 客户端，支持发送命令、自动重连和心跳检测。
// 使用 goroutine 异步处理读写，通过 channel 协调请求与响应。
// 支持超时控制和连接异常恢复。
// 负责把查询请求发出去，并把结果拿回来
package client

/*
+------------------+
|     User Send    |
|  SET key value   |
+--------+---------+
         |
         v
+------------------+     +------------------+
|  pendingReqs     | --> |  handlerWrite    | --> 写入 conn
| (待发送请求)      |     | (发送请求)        |
+------------------+     +--------+---------+
                                   |
                                   v
                            Redis Server

                                   |
                                   v
+------------------+     +------------------+
|  handlerRead     | <-- |   ParseStream    | <-- 读取 conn
| (处理响应)        |     | (解析响应)        |
+--------+---------+     +------------------+
         |
         v
+------------------+
|  waitingReqs     | <-- 匹配请求并唤醒
| (等待响应的请求)  |
+------------------+
         |
         v
     返回 reply 给用户

+------------------+
|   heartbeat      | --每10s--> PING
+------------------+
*/

import (
	"errors"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/sync/wait"
	"myredis/myredis/parser"
	"myredis/protocol"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

const (
	maxWait  = 3 * time.Second
	chanSize = 256
)

type Client struct {
	conn        net.Conn
	pendingReqs chan *request // 待发送请求的缓冲队列
	waitingReqs chan *request // 等待响应的请求队列

	addr   string
	status int32

	ticker  *time.Ticker
	working *sync.WaitGroup // 正在处理的请求计数
}

// 封装发送到 redis 服务器的一个请求
type request struct {
	id        uint64
	args      [][]byte
	reply     myredis.Reply // 存储服务器返回的响应
	heartbeat bool          // 是否是心跳请求
	waiting   *wait.Wait    // 用于同步等待响应
	err       error         // 请求过程中的错误
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:        conn,
		addr:        addr,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

func (client *Client) RemoteAddress() string {
	return client.addr
}

func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handlerWrite()
	go client.handlerRead()
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

// 向对应的服务器发送 redis 命令并同步等待回复
func (client *Client) Send(args [][]byte) myredis.Reply {
	// 检测连接状态
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}
	// 构造请求
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	// 加入发送缓冲队列
	client.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil {
		return protocol.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

// 关闭客户端连接
//
// 顺序：切换状态/关闭定时器/关闭发送缓冲/关闭连接和结果队列
func (client *Client) Close() {
	// 切换状态，关闭定时器
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()
	close(client.pendingReqs)

	// 等待所有运行中的连接完成
	client.working.Wait()

	_ = client.conn.Close()
	close(client.waitingReqs)
}

// 从 pendingReqs 取请求并写入网络
func (client *Client) handlerWrite() {
	// 遍历发送缓冲区，发送请求
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	// 转为 redis 回复
	msg := protocol.MakeMultiBulkReply(req.args)
	msgBytes := msg.ToBytes()
	var err error

	for i := 0; i < 3; i++ {
		_, err = client.conn.Write(msgBytes)
		// 正确发送，结束，否则重新发送
		if err == nil || (!strings.Contains(err.Error(), "timeout") &&
			!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil {
		// 发送成功，加入等待响应队列
		client.waitingReqs <- req
	} else {
		// 发送失败，直接唤醒调用方
		req.err = err
		req.waiting.Done()
	}
}

// 处理心跳
func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) doHeartbeat() {
	req := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	// 加入发送队列
	client.pendingReqs <- req
	req.waiting.WaitWithTimeout(maxWait)
}

// 读取响应并完成对应请求
func (client *Client) handlerRead() {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		// 检测到了错误，尝试自动重连
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}

func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	_ = client.conn.Close() // 尝试关闭连接

	var conn net.Conn
	// 尝试重连（三次）
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("times: " + strconv.Itoa(i) + " reconnect error: " + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	// 达到最大次数依然失败
	if conn == nil {
		client.Close()
		return
	}
	client.conn = conn
	// 重新更新状态
	close(client.waitingReqs)
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}
	client.waitingReqs = make(chan *request, chanSize)
	go client.handlerRead()
}

// 匹配响应并唤醒等待的请求
func (client *Client) finishRequest(reply myredis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.waitingReqs // 取出最早发出但未完成的请求
	if request == nil {
		return
	}
	request.reply = reply // 填充响应
	if request.waiting != nil {
		request.waiting.Done() // 唤醒等待的 Send() 调用
	}
}
