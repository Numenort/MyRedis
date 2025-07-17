package protocol

import (
	"bytes"
	"myredis/interface/myredis"
)

/*
	如果这个结构体永远不会携带任何状态
	（比如 OkReply 只返回固定的 +OK\r\n 字节）
	那么可以只创建一个实例，重复使用。
*/

// 处理 PING 命令的响应
type PongReply struct{}

var PongBytes = []byte("+PONG\r\n")

func (r *PongReply) ToBytes() []byte {
	return PongBytes
}

// 执行成功
type OkReply struct{}

var OkBytes = []byte("+OK\r\n")

func (r *OkReply) ToBytes() []byte {
	return OkBytes
}

var theOkReply = new(OkReply)

func MakeOkReply() *OkReply {
	return theOkReply
}

// 访问一个不存在的键时返回此响应
type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n")

func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

// 访问一个不存在的键时返回此响应
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// 用于表示空列表或空集合等数据结构
var emptyMultiBulkBytes = []byte("*0\r\n")

type EmptyMultiBulkReply struct{}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

func IsEmptyMultiBulkReply(reply myredis.Reply) bool {
	return bytes.Equal(reply.ToBytes(), emptyMultiBulkBytes)
}

// 有些命令不返回任何内容
type NoReply struct{}

var noBytes = []byte("")

func (r *NoReply) ToBytes() []byte {
	return noBytes
}

// 用于 Redis 事务中，表示命令已入队等待执行
type QueuedReply struct{}

var queuedBytes = []byte("+QUEUED\r\n")

func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

var theQueuedReply = new(QueuedReply)

func MakeQueuedReply() *QueuedReply {
	return theQueuedReply
}
