package database

import (
	"math"
	"myredis/lib/utils"
	"myredis/protocol"
	"myredis/protocol/assert"
	"strconv"
	"testing"
)

func TestHSet(t *testing.T) {
	testDB.Flush()
	size := 100

	// test hset
	key := utils.RandString(10)
	values := make(map[string][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		field := strconv.Itoa(i)
		values[field] = []byte(value)
		result := testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
		if intResult, _ := result.(*protocol.IntReply); intResult.Code != int64(1) {
			t.Errorf("expected %d, actually %d", 1, intResult.Code)
		}
	}

	// test hget, hexists and hstrlen
	for field, v := range values {
		actual := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
		expected := protocol.MakeBulkReply(v)
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes()))
		}
		actual = testDB.Exec(nil, utils.ToCmdLine("hexists", key, field))
		if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(1) {
			t.Errorf("expected %d, actually %d", 1, intResult.Code)
		}

		actual = testDB.Exec(nil, utils.ToCmdLine("hstrlen", key, field))
		if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(len(v)) {
			t.Errorf("expected %d, actually %d", int64(len(v)), intResult.Code)
		}
	}

	// test hlen
	actual := testDB.Exec(nil, utils.ToCmdLine("hlen", key))
	if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(len(values)) {
		t.Errorf("expected %d, actually %d", len(values), intResult.Code)
	}
}

func TestHSetNX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)
	value := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("hsetnx", key, field, value))
	assert.AssertIntReply(t, result, 1)
	value2 := utils.RandString(10)
	result = testDB.Exec(nil, utils.ToCmdLine("hsetnx", key, field, value2))
	assert.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	assert.AssertBulkReply(t, result, value)
}

func TestUndoHSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
	cmdLine := utils.ToCmdLine("hset", key, field, value2)
	undoCmdLines := undoHSet(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	assert.AssertBulkReply(t, result, value)
}

func TestHDel(t *testing.T) {
	testDB.Flush()
	size := 100

	// set values
	key := utils.RandString(10)
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		field := strconv.Itoa(i)
		fields[i] = field
		testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
	}

	// test HDel
	args := []string{key}
	args = append(args, fields...)
	actual := testDB.Exec(nil, utils.ToCmdLine2("hdel", args...))
	if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(len(fields)) {
		t.Errorf("expected %d, actually %d", len(fields), intResult.Code)
	}

	actual = testDB.Exec(nil, utils.ToCmdLine("hlen", key))
	if intResult, _ := actual.(*protocol.IntReply); intResult.Code != int64(0) {
		t.Errorf("expected %d, actually %d", 0, intResult.Code)
	}
}

func TestUndoHDel(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)
	value := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hset", key, field, value))
	cmdLine := utils.ToCmdLine("hdel", key, field)
	undoCmdLines := undoHDel(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	assert.AssertBulkReply(t, result, value)
}

func TestHMSet(t *testing.T) {
	testDB.Flush()
	size := 100

	// test hset
	key := utils.RandString(10)
	fields := make([]string, size)
	values := make([]string, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		fields[i] = utils.RandString(10)
		values[i] = utils.RandString(10)
		setArgs = append(setArgs, fields[i], values[i])
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("hmset", setArgs...))
	if _, ok := result.(*protocol.OkReply); !ok {
		t.Errorf("expected ok, actually %s", string(result.ToBytes()))
	}

	// test HMGet
	getArgs := []string{key}
	getArgs = append(getArgs, fields...)
	actual := testDB.Exec(nil, utils.ToCmdLine2("hmget", getArgs...))
	expected := protocol.MakeMultiBulkReply(utils.ToCmdLine(values...))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Errorf("expected %s, actually %s", string(expected.ToBytes()), string(actual.ToBytes()))
	}
}

func TestUndoHMSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field1 := utils.RandString(10)
	field2 := utils.RandString(10)
	value := utils.RandString(10)
	value2 := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hmset", key, field1, value, field2, value))
	cmdLine := utils.ToCmdLine("hmset", key, field1, value2, field2, value2)
	undoCmdLines := undoHMSet(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field1))
	assert.AssertBulkReply(t, result, value)
	result = testDB.Exec(nil, utils.ToCmdLine("hget", key, field2))
	assert.AssertBulkReply(t, result, value)
}

func TestHIncrBy(t *testing.T) {
	testDB.Flush()

	key := utils.RandString(10)
	result := testDB.Exec(nil, utils.ToCmdLine("hincrby", key, "a", "1"))
	if bulkResult, _ := result.(*protocol.BulkReply); string(bulkResult.Arg) != "1" {
		t.Errorf("expected %s, actually %s", "1", string(bulkResult.Arg))
	}
	result = testDB.Exec(nil, utils.ToCmdLine("hincrby", key, "a", "1"))
	if bulkResult, _ := result.(*protocol.BulkReply); string(bulkResult.Arg) != "2" {
		t.Errorf("expected %s, actually %s", "2", string(bulkResult.Arg))
	}

	result = testDB.Exec(nil, utils.ToCmdLine("hincrbyfloat", key, "b", "1.2"))

	if bulkResult, ok := result.(*protocol.BulkReply); ok {
		val, _ := strconv.ParseFloat(string(bulkResult.Arg), 10)
		if math.Abs(val-1.2) > 1e-4 {
			t.Errorf("expected %s, actually %s", "1.2", string(bulkResult.Arg))
		}
	} else {
		t.Errorf("error happens cause: %s", result.ToBytes())
	}
	result = testDB.Exec(nil, utils.ToCmdLine("hincrbyfloat", key, "b", "1.2"))
	if bulkResult, ok := result.(*protocol.BulkReply); ok {
		val, _ := strconv.ParseFloat(string(bulkResult.Arg), 10)
		if math.Abs(val-2.4) > 1e-4 {
			t.Errorf("expected %s, actually %s", "1.2", string(bulkResult.Arg))
		}
	} else {
		t.Errorf("error happens cause: %s", result.ToBytes())
	}
}

func TestUndoHIncr(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	field := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("hset", key, field, "1"))
	cmdLine := utils.ToCmdLine("hinctby", key, field, "2")
	undoCmdLines := undoHIncr(testDB, cmdLine[1:])
	testDB.Exec(nil, cmdLine)
	for _, cmdLine := range undoCmdLines {
		testDB.Exec(nil, cmdLine)
	}
	result := testDB.Exec(nil, utils.ToCmdLine("hget", key, field))
	assert.AssertBulkReply(t, result, "1")
}
