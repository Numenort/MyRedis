package command

import (
	"myredis/cluster/core"
	"myredis/interface/myredis"
	"myredis/protocol"
)

func execMSetInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("mset")
	}
	cmdLine[0] = []byte("mset")
	return cluster.LocalExec(c, cmdLine)
}

func execMGetInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeArgNumErrReply("mget")
	}
	cmdLine[0] = []byte("mget")
	return cluster.LocalExec(c, cmdLine)
}

func execMSetNxInLocal(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("msetnx")
	}
	cmdLine[0] = []byte("msetnx")
	return cluster.LocalExec(c, cmdLine)
}

func execMSet(cluster *core.Cluster, c myredis.Connection, cmdLine CmdLine) myredis.Reply {
	return nil
}

func init() {
	core.RegisterCmd("mset_", execMSetInLocal)
	core.RegisterCmd("mget_", execMGetInLocal)
	core.RegisterCmd("msetnx_", execMSetNxInLocal)
}
