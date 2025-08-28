package database

import (
	"math/rand"
	"myredis/config"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/protocol/assert"
	"testing"
	"time"
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

func TestAuth(t *testing.T) {
	passwd := utils.RandString(10)
	c := connection.NewSimpleConn()
	ret := testServer.Exec(c, utils.ToCmdLine("AUTH"))
	assert.AssertErrReply(t, ret, "ERR wrong number of arguments for 'auth' command")
	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd))
	assert.AssertErrReply(t, ret, "ERR Client sent AUTH, but no password is set")

	config.Properties.RequirePass = passwd
	defer func() {
		config.Properties.RequirePass = ""
	}()
	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd+"wrong"))
	assert.AssertErrReply(t, ret, "ERR invalid password")
	ret = testServer.Exec(c, utils.ToCmdLine("GET", "A"))
	assert.AssertErrReply(t, ret, "NOAUTH Authentication required")
	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd))
	assert.AssertStatusReply(t, ret, "OK")
}

func TestInfo(t *testing.T) {
	c := connection.NewSimpleConn()
	ret := testServer.Exec(c, utils.ToCmdLine("INFO"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "server"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "client"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "cluster"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("iNFO", "SeRvEr"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "Keyspace"))
	assert.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("iNFO", "abc", "bde"))
	assert.AssertErrReply(t, ret, "ERR wrong number of arguments for 'info' command")
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "abc"))
	assert.AssertErrReply(t, ret, "Invalid section for 'info' command")
}

func TestDbSize(t *testing.T) {
	c := connection.NewSimpleConn()
	rand.NewSource(time.Now().UnixNano())
	randomNum := rand.Intn(10) + 1
	for i := 0; i < randomNum; i++ {
		key := utils.RandString(10)
		value := utils.RandString(10)
		testServer.Exec(c, utils.ToCmdLine("SET", key, value))
	}
	ret := testServer.Exec(c, utils.ToCmdLine("dbsize"))
	assert.AssertIntReply(t, ret, randomNum)
}
