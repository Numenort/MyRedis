package aof

import (
	"myredis/interface/database"
	"myredis/protocol"
)

func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	return nil
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	// String: key 只对应一个 String
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, args [][]byte) *protocol.MultiBulkReply {

}
