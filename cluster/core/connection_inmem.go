// 在单进程内模拟多个 Redis 集群节点之间的网络通信
// 主要功能：
// - 模拟节点间通信（peer-to-peer）
// - 支持命令转发（Relay）
// - 支持流式响应（Stream），用于 `PSYNC`、`SSCAN` 等命令

package core

import (
	"myredis/interface/myredis"
	"myredis/myredis/connection"
	"myredis/myredis/parser"
	"sync"
)

type InMemConnectionFactory struct {
	nodes map[string]*Cluster
	mu    sync.Mutex
}

func NewInMemConnectionFactory() *InMemConnectionFactory {
	return &InMemConnectionFactory{
		nodes: make(map[string]*Cluster),
	}
}

type InMemClient struct {
	addr    string
	cluster *Cluster
}

func (c *InMemClient) RemoteAddress() string {
	return c.addr
}

func (c *InMemClient) Send(args [][]byte) myredis.Reply {
	simpleConn := connection.NewSimpleConn()
	return c.cluster.Exec(simpleConn, args)
}

type InMemStream struct {
	conn    *connection.SimpleConn
	cluster *Cluster
}

func (c *InMemStream) Stream() <-chan *parser.Payload {
	return parser.ParseStream(c.conn)
}

func (c *InMemStream) Close() error {
	return nil
}

func (factory *InMemConnectionFactory) NewPeerClient(peerAddr string) (peerClient, error) {
	factory.mu.Lock()
	cluster := factory.nodes[peerAddr]
	factory.mu.Unlock()
	return &InMemClient{
		addr:    peerAddr,
		cluster: cluster,
	}, nil
}

func (factory *InMemConnectionFactory) BorrowPeerClient(peerAddr string) (peerClient, error) {
	return factory.NewPeerClient(peerAddr)
}

func (factory *InMemConnectionFactory) ReturnPeerClient(peerClient peerClient) error {
	return nil
}

func (factory *InMemConnectionFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	factory.mu.Lock()
	cluster := factory.nodes[peerAddr]
	factory.mu.Unlock()

	conn := connection.NewSimpleConn()

	reply := cluster.Exec(conn, cmdLine)
	conn.Write(reply.ToBytes())

	return &InMemStream{
		conn:    conn,
		cluster: cluster,
	}, nil
}

func (factory *InMemConnectionFactory) Close() error {
	return nil
}
