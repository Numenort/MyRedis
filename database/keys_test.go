package database

import (
	"myredis/lib/utils"
	"myredis/protocol"
	"myredis/protocol/assert"
	"strconv"
	"testing"
	"time"
)

func TestExists(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("exists", key))
	assert.AssertIntReply(t, result, 1)
	key = utils.RandString(10)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	assert.AssertIntReply(t, result, 0)
}

func TestType(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("type", key))
	assert.AssertStatusReply(t, result, "string")

	testDB.Remove(key)
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	assert.AssertStatusReply(t, result, "none")
	execRPush(testDB, utils.ToCmdLine(key, value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	assert.AssertStatusReply(t, result, "list")

	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("hset", key, key, value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	assert.AssertStatusReply(t, result, "hash")

	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("sadd", key, value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	assert.AssertStatusReply(t, result, "set")

	testDB.Remove(key)
	testDB.Exec(nil, utils.ToCmdLine("zadd", key, "1", value))
	result = testDB.Exec(nil, utils.ToCmdLine("type", key))
	assert.AssertStatusReply(t, result, "zset")
}

func TestRename(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))
	result := testDB.Exec(nil, utils.ToCmdLine("rename", key, newKey))
	if _, ok := result.(*protocol.OkReply); !ok {
		t.Error("expect ok")
		return
	}
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

func TestRenameNx(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))
	result := testDB.Exec(nil, utils.ToCmdLine("RenameNx", key, newKey))
	assert.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	assert.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", newKey))
	assert.AssertIntReply(t, result, 1)
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

func TestTTL(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))

	result := testDB.Exec(nil, utils.ToCmdLine("expire", key, "1000"))
	assert.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("persist", key))
	assert.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	assert.AssertIntReply(t, result, -1)

	result = testDB.Exec(nil, utils.ToCmdLine("PExpire", key, "1000000"))
	assert.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("PTTL", key))
	intResult, ok = result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestExpire(t *testing.T) {
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
	testDB.Exec(nil, utils.ToCmdLine("PEXPIRE", key, "100"))
	time.Sleep(2 * time.Second)
	result := testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	assert.AssertIntReply(t, result, -2)

}

func TestExpireAt(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))

	expireAt := time.Now().Add(time.Minute).Unix()
	result := testDB.Exec(nil, utils.ToCmdLine("ExpireAt", key, strconv.FormatInt(expireAt, 10)))

	assert.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	expireAt = time.Now().Add(time.Minute).Unix()
	result = testDB.Exec(nil, utils.ToCmdLine("PExpireAt", key, strconv.FormatInt(expireAt*1000, 10)))
	assert.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	intResult, ok = result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestExpiredTime(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))

	result := testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	assert.AssertIntReply(t, result, -1)
	result = testDB.Exec(nil, utils.ToCmdLine("EXPIRETIME", key))
	assert.AssertIntReply(t, result, -1)
	result = testDB.Exec(nil, utils.ToCmdLine("PEXPIRETIME", key))
	assert.AssertIntReply(t, result, -1)

	estimateExpireTimestamp := time.Now().Add(2 * time.Second).Unix() // actually expiration may be >= estimateExpireTimestamp
	testDB.Exec(nil, utils.ToCmdLine("EXPIRE", key, "2"))
	//tt := time.Now()
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code < 0 || intResult.Code > 2 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
	result = testDB.Exec(nil, utils.ToCmdLine("EXPIRETIME", key))
	intResult, ok = result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code < estimateExpireTimestamp {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	result = testDB.Exec(nil, utils.ToCmdLine("PEXPIRETIME", key))
	intResult, ok = result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code < estimateExpireTimestamp*1000 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}

	time.Sleep(3 * time.Second)
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", key))
	assert.AssertIntReply(t, result, -2)
	result = testDB.Exec(nil, utils.ToCmdLine("EXPIRETIME", key))
	assert.AssertIntReply(t, result, -2)
	intResult, ok = result.(*protocol.IntReply)
	result = testDB.Exec(nil, utils.ToCmdLine("PEXPIRETIME", key))
	assert.AssertIntReply(t, result, -2)
	intResult, ok = result.(*protocol.IntReply)

}

func TestKeys(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "a:"+key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "b:"+key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "b:"+key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "c:"+key, value, "EX", "0"))
	time.Sleep(time.Second)

	result := testDB.Exec(nil, utils.ToCmdLine("keys", "*"))
	assert.AssertMultiBulkReplySize(t, result, 3)
	result = testDB.Exec(nil, utils.ToCmdLine("keys", "a:*"))
	assert.AssertMultiBulkReplySize(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("keys", "?:*"))
	assert.AssertMultiBulkReplySize(t, result, 2)
}
