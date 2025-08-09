package core

import (
	"myredis/config"
	"myredis/database"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/protocol"
	"strings"
)

type CmdFunc func(cluster *Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply

var commands = make(map[string]CmdFunc)

func RegisterCmd(names string, cmd CmdFunc) {
	name := strings.ToLower(names)
	commands[name] = cmd
}

func (cluster *Cluster) Exec(c myredis.Connection, cmdLine [][]byte) (result myredis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return database.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return
	}
}

func isAuthenticated(c myredis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}
