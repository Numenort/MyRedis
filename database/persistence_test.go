package database

import (
	"myredis/aof"
	"myredis/config"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/protocol"
	"myredis/protocol/assert"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestLoadRDB(t *testing.T) {
	_, b, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(b))
	os.Chdir(projectRoot)
	config.Properties = &config.ServerProperties{
		AppendOnly:  false,
		RDBFilename: "test.rdb", // set working directory to project root
	}
	conn := connection.NewSimpleConn()
	rdbDB := NewStandaloneServer()
	result := rdbDB.Exec(conn, utils.ToCmdLine("Get", "str"))
	assert.AssertBulkReply(t, result, "str")
	result = rdbDB.Exec(conn, utils.ToCmdLine("LRange", "list", "0", "-1"))
	assert.AssertMultiBulkReply(t, result, []string{"1", "2", "3", "4"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("HGetAll", "hash"))
	assert.AssertMultiBulkReply(t, result, []string{"1", "1"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("ZRange", "zset", "0", "-1", "WITHSCORES"))
	assert.AssertMultiBulkReply(t, result, []string{"0", "1", "1", "1"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("SCard", "set"))
	assert.AssertIntReply(t, result, 1)

	// test no rdb file
	config.Properties = &config.ServerProperties{
		AppendOnly:  false,
		RDBFilename: "noexists.rdb",
	}
	rdbDB = NewStandaloneServer()
	result = rdbDB.Exec(conn, utils.ToCmdLine("Get", "str"))
	assert.AssertNullBulk(t, result)
}

func TestServerFsyncAlways(t *testing.T) {
	aofFile, err := os.CreateTemp("", "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	config.Properties.AppendOnly = true
	config.Properties.AppendFilename = aofFile.Name()
	config.Properties.AppendFsync = aof.FsyncAlways
	server := NewStandaloneServer()
	conn := connection.NewSimpleConn()
	server.Exec(conn, utils.ToCmdLine("del", "1"))
	ret := server.Exec(conn, utils.ToCmdLine("incr", "1"))
	assert.AssertNotError(t, ret)
	reader := NewStandaloneServer()
	ret = reader.Exec(conn, utils.ToCmdLine("get", "1"))
	assert.AssertBulkReply(t, ret, "1")
}

func TestServerFsyncEverySec(t *testing.T) {
	aofFile, err := os.CreateTemp("", "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	config.Properties.AppendOnly = true
	config.Properties.AppendFilename = aofFile.Name()
	config.Properties.AppendFsync = aof.FsyncEverySec
	server := NewStandaloneServer()
	conn := connection.NewSimpleConn()
	// --- 测试 1: 等待 fsync 后能恢复数据---
	server.Exec(conn, utils.ToCmdLine("del", "1"))
	ret := server.Exec(conn, utils.ToCmdLine("incr", "1"))
	assert.AssertNotError(t, ret)

	// 等待超过 1 秒，确保后台 fsync 执行
	time.Sleep(1500 * time.Millisecond)

	// 正常关闭
	server.Close()

	// 重启读取
	reader := NewStandaloneServer()
	ret = reader.Exec(conn, utils.ToCmdLine("get", "1"))
	assert.AssertBulkReply(t, ret, "1") //
	reader.Close()

	// --- 测试 2: 不等待 fsync，立即关闭 → 数据可能丢失 ---
	server = NewStandaloneServer()
	server.Exec(conn, utils.ToCmdLine("set", "will_be_lost", "value"))

	server.Close()

	// 重启服务
	reader = NewStandaloneServer()
	ret = reader.Exec(conn, utils.ToCmdLine("get", "will_be_lost"))

	if bulk, ok := ret.(*protocol.BulkReply); ok {
		if bulk.Arg != nil {
			t.Log("data survived (within fsync window), acceptable")
		}
	} else {
		// 如果是 nil，说明数据丢失，符合 FsyncEverySec 的预期
		assert.AssertNullBulk(t, ret)
		t.Log("data lost on restart, as expected in crash scenario")
	}

	reader.Close()
}
