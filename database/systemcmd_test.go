package database

import (
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/protocol/assert"
	"testing"
)

func TestPing(t *testing.T) {
	c := connection.NewSimpleConn()
	actual := Ping(c, utils.ToCmdLine())
	assert.AssertStatusReply(t, actual, "PONG")
	val := utils.RandString(5)
	actual = Ping(c, utils.ToCmdLine(val))
	assert.AssertStatusReply(t, actual, val)
	// print(string(actual.ToBytes()))
	actual = Ping(c, utils.ToCmdLine(val, val))
	// print(string(actual.ToBytes()))
	assert.AssertErrReply(t, actual, "ERR wrong number of arguments for 'ping' command")
}

// func TestAuth(t *testing.T) {
// 	passwd := utils.RandString(10)
// 	c := connection.NewSimpleConn()
// 	ret := testServer.Exec(c, utils.ToCmdLine("AUTH"))
// 	asserts.AssertErrReply(t, ret, "ERR wrong number of arguments for 'auth' command")
// 	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd))
// 	asserts.AssertErrReply(t, ret, "ERR Client sent AUTH, but no password is set")

// 	config.Properties.RequirePass = passwd
// 	defer func() {
// 		config.Properties.RequirePass = ""
// 	}()
// 	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd+"wrong"))
// 	asserts.AssertErrReply(t, ret, "ERR invalid password")
// 	ret = testServer.Exec(c, utils.ToCmdLine("GET", "A"))
// 	asserts.AssertErrReply(t, ret, "NOAUTH Authentication required")
// 	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd))
// 	asserts.AssertStatusReply(t, ret, "OK")

// }
