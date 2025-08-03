package database

import (
	"myredis/aof"
	"myredis/config"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/protocol"
	"myredis/protocol/assert"
	"os"
	"path"
	"strconv"
	"testing"
)

func makeTestData(db database.DB, dbIndex int, prefix string, size int) {
	conn := connection.NewSimpleConn()
	conn.SelectDB(dbIndex)
	db.Exec(conn, utils.ToCmdLine("FlushDB"))
	cursor := 0
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("SET", key, key, "EX", "10000"))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("RPUSH", key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("HSET", key, key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("SADD", key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("ZADD", key, "10", key))
	}
}

func validateTestData(t *testing.T, db database.DB, dbIndex int, prefix string, size int) {
	conn := connection.NewSimpleConn()
	conn.SelectDB(dbIndex)
	cursor := 0
	var ret myredis.Reply
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("GET", key))
		assert.AssertBulkReply(t, ret, key)
		ret = db.Exec(conn, utils.ToCmdLine("TTL", key))
		intResult, ok := ret.(*protocol.IntReply)
		if !ok {
			t.Errorf("expected int protocol, actually %s", ret.ToBytes())
			return
		}
		if intResult.Code <= 0 || intResult.Code > 10000 {
			t.Error("wrong ttl")
		}
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("LRANGE", key, "0", "-1"))
		assert.AssertMultiBulkReply(t, ret, []string{key})
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("HGET", key, key))
		assert.AssertBulkReply(t, ret, key)
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("SIsMember", key, key))
		assert.AssertIntReply(t, ret, 1)
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("ZRANGE", key, "0", "-1"))
		assert.AssertMultiBulkReply(t, ret, []string{key})
	}
}

func TestAof(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tempDir, "a.aof")
	defer os.Remove(aofFilename)

	config.Properties = &config.ServerProperties{
		AppendOnly:        true,
		AppendFilename:    aofFilename,
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncEverySec,
	}
	dbNum := 4
	size := 10
	var prefixes []string
	aofWriteDB := NewStandaloneServer()
	// generate test data
	for i := 0; i < dbNum; i++ {
		prefix := utils.RandString(8)
		prefixes = append(prefixes, prefix)
		makeTestData(aofWriteDB, i, prefix, size)
	}
	aofWriteDB.Close()                 // wait for aof finished
	aofReadDB := NewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		prefix := prefixes[i]
		validateTestData(t, aofReadDB, i, prefix, size)
	}
	aofReadDB.Close()
}
