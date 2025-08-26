package database

import (
	"myredis/lib/utils"
	"myredis/protocol"
	"myredis/protocol/assert"
	"testing"
)

func TestExistsIn(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("ExistIn", key, key2))
	assert.AssertMultiBulkReply(t, result, []string{key})
	key3 := utils.RandString(10)
	result = testDB.Exec(nil, utils.ToCmdLine("ExistIn", key2, key3))
	assert.AssertMultiBulkReplySize(t, result, 0)
}

func TestDumpKeyAndRenameTo(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))

	result := testDB.Exec(nil, utils.ToCmdLine("DumpKey", key))
	if protocol.IsErrorReply(result) {
		t.Error("dump key error")
		return
	}
	dumpResult := result.(*protocol.MultiBulkReply)
	result = testDB.Exec(nil, utils.ToCmdLine("RenameTo", newKey,
		string(dumpResult.Args[0]), string(dumpResult.Args[1])))
	assert.AssertNotError(t, result)
	result = testDB.Exec(nil, utils.ToCmdLine("RenameFrom", key))
	assert.AssertNotError(t, result)

	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	assert.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", newKey))
	assert.AssertIntReply(t, result, 1)
	// check ttl
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", newKey))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}
