package database

import (
	"myredis/config"
	"myredis/interface/myredis"
	"myredis/protocol"
)

// Ping 处理 PING 命令：无参数返回 PONG，一个参数则回显该参数，否则报错。
func Ping(c myredis.Connection, args [][]byte) myredis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// Auth 处理 AUTH 命令：校验密码是否正确。服务器未设密码或密码错误时返回错误。
func Auth(c myredis.Connection, args [][]byte) myredis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	// 服务器未配置密码
	if config.Properties.RequirePass == "" {
		return protocol.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	password := string(args[0])
	c.SetPassword(password) // 记录客户端提交的密码
	if config.Properties.RequirePass != password {
		return protocol.MakeErrReply("ERR invalid password")
	}
	return &protocol.OkReply{}
}

func isAuthenticated(c myredis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// func Info(db *Server, args [][]byte) myredis.Reply {

// }
