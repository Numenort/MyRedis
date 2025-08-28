package server

import (
	"context"
	"io"
	"myredis/cluster"
	"myredis/config"
	database2 "myredis/database"
	"myredis/interface/database"
	"myredis/lib/logger"
	"myredis/lib/sync/atomic"
	"myredis/myredis/connection"
	"myredis/myredis/parser"
	"myredis/protocol"
	"net"
	"strings"
	"sync"
)

var errUnknownReplyBytes = []byte("-ERR unknown\r\n")

type Handler struct {
	activateConn sync.Map    // 存储所有活跃连接
	db           database.DB // 数据库实例
	closing      atomic.Boolean
}

func MakeHandler() *Handler {
	var db database.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster()
	} else {
		db = database2.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}

// closeClient 安全关闭客户端连接
// 1. 关闭底层网络连接
// 2. 通知数据库执行清理逻辑
// 3. 从活跃连接表中删除
func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activateConn.Delete(client)
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	// 将连接加入活跃连接表，用于后续管理
	h.activateConn.Store(client, struct{}{})
	// 持续输出解析结果
	ch := parser.ParseStream(conn)

	for payload := range ch {
		if payload.Err != nil {
			// 连接关闭错误，正常退出
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := conn.Write(errReply.ToBytes())
			if err != nil {
				// 写响应失败，说明连接已断，关闭客户端
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		repl, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		// 数据库执行结果
		result := h.db.Exec(client, repl.Args)
		// 将执行结果写回客户端
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(errUnknownReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.activateConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
