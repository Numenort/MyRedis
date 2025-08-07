/*
实现了 ConnectionFactory 接口，用于在 Redis 集群中管理与其他节点（peer）的 TCP 连接。
	主要功能包括：
	- 借用/归还连接（使用连接池）
	- 创建流式通信通道（适用于大数据量或持续响应场景）
	- 支持认证（AUTH）
	- 连接复用与资源回收
核心图解：
nodeConnections (dict.Dict)
│
├── "192.168.1.10:6379" → *pool.Pool  // 我为 Node A 维护的连接池
│                         │
│                         ├── conn1 (我连 A 的连接)
│                         ├── conn2
│                         └── ...（最多16个）
│
├── "192.168.1.11:6379" → *pool.Pool  // 我为 Node B 维护的连接池
│                         │
│                         ├── conn1 (我连 B 的连接)
│                         └── ...
│
└── ...                   // 其他 peer
*/

package core

import (
	"errors"
	"myredis/datastruct/dict"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/pool"
	"myredis/myredis/parser"
)

type CmdLine = [][]byte

// 管理与集群中其他节点（peer）的连接。
type ConnectionFactory interface {
	// 从连接池中获取一个到指定节点的客户端连接
	BorrowPeerClient(peerAddr string) (peerClient, error)
	// 将使用完毕的客户端连接归还给连接池
	ReturnPeerClient(peerClient peerClient) error
	// 创建一个与目标节点的长连接流通道。
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) // 创建流式连接
	Close() error
}

// 与远程 Redis 节点通信的客户端抽象（自身持有）
type peerClient interface {
	RemoteAddress() string            // 返回对端地址
	Send(args [][]byte) myredis.Reply // 发送命令，返回响应
}

type peerStream interface {
	Stream() <-chan *parser.Payload // 持续接收解析后的 Redis 协议
	Close() error
}

type defaultClientFactory struct {
	nodeConnection dict.Dict // example: 127.0.0.1:6556 -> ConnectionPool
}

var connectionPoolConfig = pool.PoolConfig{
	MaxIdle:     1,
	MaxActivate: 16,
}

// 默认连接工厂实例，适用于单线程初始化场景。
func newFactory() ConnectionFactory {
	return &defaultClientFactory{
		nodeConnection: dict.MakeSimple(),
	}
}

// 创建一个指定 peerAddr 的客户端连接
func (factory *defaultClientFactory) NewPeerClient(peerAddr string) (peerClient, error) {
	return nil, nil
}

// 从指定 peerAddr 的连接池中拿走一个客户端连接
func (factory *defaultClientFactory) BorrowPeerClient(peerAddr string) (peerClient, error) {
	var connectionPool *pool.Pool
	// 检查对应节点的连接池是否存在
	rawPool, ok := factory.nodeConnection.Get(peerAddr)
	if !ok {
		// 创建连接池
		creatorFactory := func() (interface{}, error) {
			return factory.NewPeerClient(peerAddr)
		}
		finalizer := func(x interface{}) {
			logger.Debug("destory client")
			client, ok := x.(client.Client)
			if !ok {
				return
			}
			client.Close()
		}
		connectionPool = pool.NewPool(creatorFactory, finalizer, connectionPoolConfig)
		factory.nodeConnection.Put(peerAddr, connectionPool)
	} else {
		connectionPool = rawPool.(*pool.Pool)
	}
	// 获取连接
	connection, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	// 获取对应的 peerClient
	conn, ok := connection.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

// 归还客户端连接至连接池
func (factory *defaultClientFactory) ReturnPeerClient(peerClient peerClient) error {
	// 得到为对应远程节点建立的连接池
	rawPool, ok := factory.nodeConnection.Get(peerClient.RemoteAddress())
	if !ok {
		return errors.New("connection pool not found")
	}
	rawPool.(*pool.Pool).Put(peerClient)
	return nil
}

func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	return nil, nil
}

func (factory *defaultClientFactory) Close() error {
	return nil
}
