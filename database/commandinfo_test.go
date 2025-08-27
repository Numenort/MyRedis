package database

import (
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/protocol/assert"
	"testing"
)

func TestCommandInfo(t *testing.T) {
	c := connection.NewSimpleConn()
	ret := testServer.Exec(c, utils.ToCmdLine("command"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("command", "info", "mset"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("command", "getkeys", "mset", "a", "a", "b", "b"))
	assert.AssertMultiBulkReply(t, ret, []string{"a", "b"})
	ret = testServer.Exec(c, utils.ToCmdLine("command", "foobar"))
	assert.AssertErrReply(t, ret, "Unknown subcommand 'foobar'")
}
