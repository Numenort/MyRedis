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
	"fmt"
	"myredis/config"
	"myredis/datastruct/dict"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/pool"
	"myredis/lib/utils"
	"myredis/myredis/client"
	"myredis/myredis/parser"
	"myredis/protocol"
	"net"
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

// 表示一个与远程 Redis 节点之间的流式通信通道
type peerStream interface {
	Stream() <-chan *parser.Payload // 持续接收解析后的 Redis 协议
	Close() error
}

// 封装了一个 TCP 连接和一个协议解析流，实现了 PeerStream
type tcpStream struct {
	conn net.Conn
	ch   <-chan *parser.Payload
}

func (stream *tcpStream) Stream() <-chan *parser.Payload {
	return stream.ch
}

func (stream *tcpStream) Close() error {
	return stream.conn.Close()
}

type defaultClientFactory struct {
	// 管理所有远程节点的连接池的容器
	nodeConnections dict.Dict // example: 127.0.0.1:6556 -> ConnectionPool
}

var connectionPoolConfig = pool.PoolConfig{
	MaxIdle:     1,
	MaxActivate: 16,
}

// 默认连接工厂实例，适用于单线程初始化场景。
func newFactory() ConnectionFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeSimple(),
	}
}

func newDefaultClientFactory() *defaultClientFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeConcurrent(1),
	}
}

// 创建一个指定 peerAddr 的客户端连接
func (factory *defaultClientFactory) NewPeerClient(peerAddr string) (peerClient, error) {
	client, err := client.NewClient(peerAddr)
	if err != nil {
		return nil, err
	}
	client.Start()

	if config.Properties.RequirePass != "" {
		authRep := client.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authRep) {
			return nil, fmt.Errorf("auth failed, resp: %s", string(authRep.ToBytes()))
		}
	}
	return client, nil
}

// 从指定 peerAddr 的连接池中拿走一个客户端连接
func (factory *defaultClientFactory) BorrowPeerClient(peerAddr string) (peerClient, error) {
	var connectionPool *pool.Pool
	// 检查对应节点的连接池是否存在
	rawPool, ok := factory.nodeConnections.Get(peerAddr)
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
		factory.nodeConnections.Put(peerAddr, connectionPool)
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
	rawPool, ok := factory.nodeConnections.Get(peerClient.RemoteAddress())
	if !ok {
		return errors.New("connection pool not found")
	}
	rawPool.(*pool.Pool).Put(peerClient)
	return nil
}

// 建立一个到指定远程节点的 TCP 流式连接，并发送给定的命令
func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	// 建立 TCP 连接
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, err
	}
	channel := parser.ParseStream(conn)
	send2Node := func(cmdLine CmdLine) myredis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-channel
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}
	// 如果当前集群配置了密码认证，需要先认证
	if config.Properties.RequirePass != "" {
		// 发送授权命令
		authResp := send2Node(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authResp) {
			return nil, fmt.Errorf("auth failde, resp: %s", string(authResp.ToBytes()))
		}
	}
	// 创建恢复并写入
	req := protocol.MakeMultiBulkReply(cmdLine)
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return nil, protocol.MakeErrReply("send cmdLine failed: " + err.Error())
	}
	return &tcpStream{
		conn: conn,
		ch:   channel,
	}, nil
}

func (factory *defaultClientFactory) Close() error {
	// 遍历每个远程节点的连接池，全部关闭
	factory.nodeConnections.ForEach(func(key string, val interface{}) bool {
		val.(*pool.Pool).Close()
		return true
	})
	return nil
}

// 从主节点的连接池中拿走一个客户端连接
func (cluster *Cluster) BorrowLeaderClient() (peerClient, error) {
	leaderAddr := cluster.raftNode.GetLeaderRedisAddress()
	return cluster.connections.BorrowPeerClient(leaderAddr)
}
