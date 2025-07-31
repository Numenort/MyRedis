package connection

import (
	"myredis/lib/logger"
	"myredis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

const (
	flagSlave = uint64(1 << iota)
	flagMaster
	flagMulti
)

type Connection struct {
	conn        net.Conn
	sendingData wait.Wait
	mu          sync.Mutex
	flags       uint64
	subs        map[string]bool
	password    string
	selectedDB  int
	txErrors    []error
	watching    map[string]uint32

	queue [][][]byte
}

// 新建连接池实现连接对象复用
// 这并不是传统意义上的“连接池”（如数据库连接池），
// 而是一个 对象池（Object Pool），用于复用 Connection 结构体的内存空间。
// 它的设计目标是降低频繁创建和销毁连接对象带来的内存开销。
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

	c.subs = nil
	c.password = ""
	c.queue = nil
	c.txErrors = nil
	c.selectedDB = 0
	c.watching = nil
	connObjPool.Put(c)

	return nil
}

/* ---------- Subscribe Functions ----------*/

func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		return
	} else {
		if len(c.subs) == 0 {
			return
		}
	}
	delete(c.subs, channel)
}

func (c *Connection) SubsCount() int {
	return len(c.subs)
}

func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

func (c *Connection) SetMultiState(state bool) {
	if !state {
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti
		return
	}
	c.flags |= flagMulti
}

func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

// 获取客户端 ip 地址
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

func (c *Connection) SetPassword(password string) {
	c.password = password
}

func (c *Connection) GetPassword() string {
	return c.password
}

func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}
