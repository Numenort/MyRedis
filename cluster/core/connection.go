/*
实现了 ConnectionFactory 接口，用于在 Redis 集群中管理与其他节点（peer）的 TCP 连接。
	主要功能包括：
	- 借用/归还连接（使用连接池）
	- 创建流式通信通道（适用于大数据量或持续响应场景）
	- 支持认证（AUTH）
	- 连接复用与资源回收
*/

package core

import (
	"myredis/datastruct/dict"
	"myredis/interface/myredis"
	"myredis/lib/pool"
	"myredis/myredis/parser"
)

type CmdLine = [][]byte

// 管理与集群中其他节点（peer）的连接。
type ConnectionFactory interface {
	BorrowPeerClient(peerAddr string) (peerClient, error)
	ReturnPeerClient(peerClient peerClient) error
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) // 创建流式连接
	Close() error
}

// 与远程 Redis 节点通信的客户端抽象
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

func newFactory() ConnectionFactory {
	return &defaultClientFactory{
		nodeConnection: dict.MakeSimple(),
	}
}

func (factory *defaultClientFactory) NewPeerClient(peerAddr string) (peerClient, error) {
	return nil, nil
}

func (factory *defaultClientFactory) BorrowPeerClient(peerAddr string) (peerClient, error) {
	return nil, nil
}

func (factory *defaultClientFactory) ReturnPeerClient(peerClient peerClient) error {
	return nil
}

func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	return nil, nil
}

func (factory *defaultClientFactory) Close() error {
	return nil
}
