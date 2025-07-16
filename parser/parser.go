package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"myredis/lib/logger"
	"myredis/myredis"
	"myredis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data myredis.Reply
	Err  error
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func ParseBytes(data []byte) ([]myredis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []myredis.Reply

	// 循环读取channel解析结果
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// reads data from []byte and return the first payload
func ParseOne(data []byte) (myredis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, nil
}

func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{
				Err: err,
			}
			close(ch)
			return
		}
		length := len(line)
		// 判断行是否太短（至少要包含 CRLF)
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			// Status Reply
			content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeStatusReply(content),
			}
			// 当 Redis 从节点收到主节点发送的 +FULLRESYNC 状态响应时
			// 表示即将接收一个 RDB 快照文件。解析这个紧跟其后的 RDB 数据块
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parserRDBBulkString(reader, ch)
				if err != nil {
					// 解析失败，关闭 channel
					ch <- &Payload{
						Err: err,
					}
					close(ch)
					return
				}
			}
		case '-':
			// ERR Reply
			content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeErrReply(content),
			}
		case ':':
			// int Reply
			content := string(line[1:])
			value, err := strconv.ParseInt(content, 10, 64)
			if err != nil {
				protocolError(ch, "illegal number"+content)
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		case '$':
			// string Reply
			err = parserBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
		case '*':
			// array Reply
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

// 解析不同的字符串，包含 bulk string

func parserBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {

	// $3\r\nSET\r\n   10进制，int64
	strlen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strlen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strlen == -1 {
		// 空字符串回复
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}
	body := make([]byte, strlen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

func parserRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	header, err := reader.ReadBytes('\n')
	if err != nil {
		return errors.New("failed to read bytes")
	}
	// 去除行末的 \r\n
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	if len(header) == 0 {
		return errors.New("empty header")
	}
	// 消息体长度
	strlen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strlen <= 0 {
		return errors.New("illegal bulk header: " + string(header))
	}
	body := make([]byte, strlen)
	// 读入消息体
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)]),
	}
	return nil
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		// 类似于 Bulk String
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return nil
}

// 封装错误，通过 chan 传递到 PayLoad
func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
