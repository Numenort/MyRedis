package protocol

import (
	"bytes"
	"errors"
	"myredis/interface/myredis"
	"strconv"
)

var CRLF = "\r\n"

/*
BulkReply: 批量字符串回复
*/
type BulkReply struct {
	Arg []byte
}

// 字符串回复
func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	// $5\r\nmamba\r\n
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/*
MultiBulkReply: 多个 Bulk 字符串组成的数组
*/
type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

// *2\r\n
// $5\r\n
// hello\r\n
// $5\r\n
// world\r\n

func (r *MultiBulkReply) ToBytes() []byte {
	var buf bytes.Buffer

	// * + len + CRLF
	argLen := len(r.Args)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2

	for _, arg := range r.Args {
		if arg == nil {
			bufLen += 2
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2
		}
	}

	// 分配缓冲区空间
	buf.Grow(bufLen)
	// 写入
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(argLen))
	buf.WriteString(CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			buf.Write(arg)
			buf.WriteString(CRLF)
		}
	}
	return buf.Bytes()
}

// 存储已经完成解析的 RESP 协议
type MultiRawReply struct {
	Replies []myredis.Reply
}

func MakeMultiRawReply(replies []myredis.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

/* ---- Status Reply ---- */

// 状态回复
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// +OK\r\n
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func IsOKReply(reply myredis.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

/* ---- Int Reply ---- */

// 整数回复（Integer）
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// :1000\r\n
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

// *IntReply 实现了 reply 接口，IntReply 没有

/* ---- Error Reply ---- */

// ErrorReply is an error and redis.Reply
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type StandardErrReply struct {
	Status string
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

func IsErrorReply(reply myredis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

func Try2ErrorReply(reply myredis.Reply) error {
	str := string(reply.ToBytes())
	if len(str) == 0 {
		return errors.New("empty reply")
	}
	if str[0] != '-' {
		return nil
	}
	return errors.New(str[1:])
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}
