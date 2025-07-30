package connection

import (
	"myredis/lib/logger"
	"myredis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// 这并不是传统意义上的“连接池”（如数据库连接池），
// 而是一个 对象池（Object Pool），用于复用 Connection 结构体的内存空间。
// 它的设计目标是降低频繁创建和销毁连接对象带来的内存开销。

type Connection struct {
	conn net.Conn
	// 用于优雅关闭连接
	sendingData wait.Wait

	mu sync.Mutex

	flags uint64

	password string

	// 选择的数据库
	selectedDB int
}

// 新建连接池实现连接对象复用
var connObjPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// 新建连接
func NewConn(conn net.Conn) *Connection {
	connection, ok := connObjPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	connection.conn = conn
	return connection
}

// 优雅关闭连接
func (c *Connection) Close() error {
	// 最多等待 10 秒
	c.sendingData.WaitWithTimeout(10 * time.Second)
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return err
		}
	}
	c.password = ""
	// 返回对象池
	connObjPool.Put(c)
	return nil
}

func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(b)
}

// 获取客户端 ip 地址
func (c *Connection) RemoteAddr() string {
	return c.RemoteAddr()
}

func (c *Connection) SetPassword(password string) {
	c.password = password
}

func (c *Connection) GetPassword() string {
	return c.password
}
