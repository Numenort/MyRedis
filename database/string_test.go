package database

import (
	"math"
	"myredis/lib/utils"
	"myredis/protocol"
	"myredis/protocol/assert"
	"strconv"
	"testing"
)

var testDB = makeTestDB()

// var testServer = NewStandaloneServer()

func TestSetEmpty(t *testing.T) {
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SET", key, ""))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	bulkReply, ok := actual.(*protocol.BulkReply)
	if !ok {
		t.Errorf("expected bulk protocol, actually %s", actual.ToBytes())
		return
	}
	if !(bulkReply.Arg != nil && len(bulkReply.Arg) == 0) {
		t.Error("illegal empty string")
	}
}

func TestSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)

	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected := protocol.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set NX
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "NX"))
	if _, ok := actual.(*protocol.NullBulkReply); !ok {
		t.Error("expected true actual false")
	}

	testDB.Flush()
	key = utils.RandString(10)
	value = utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "NX"))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected = protocol.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// set XX
	testDB.Flush()
	key = utils.RandString(10)
	value = utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "XX"))
	if _, ok := actual.(*protocol.NullBulkReply); !ok {
		t.Error("expected true actually false ")
	}

	execSet(testDB, utils.ToCmdLine(key, value))
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))
	actual = testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "XX"))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	assert.AssertBulkReply(t, actual, value)

	// set EX
	testDB.Remove(key)
	ttl := "1000"
	testDB.Exec(nil, utils.ToCmdLine("SET", key, value, "EX", ttl))
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	assert.AssertBulkReply(t, actual, value)
	actual = execTTL(testDB, utils.ToCmdLine(key))
	print(actual.ToBytes())
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	print(actual.ToBytes())
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
		return
	}

}

func TestSetNX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("SETNX", key, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	expected := protocol.MakeBulkReply([]byte(value))
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	actual = testDB.Exec(nil, utils.ToCmdLine("SETNX", key, value))
	expected2 := protocol.MakeIntReply(int64(0))
	if !utils.BytesEquals(actual.ToBytes(), expected2.ToBytes()) {
		t.Error("expected: " + string(expected2.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}
}

func TestSetEX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	ttl := "1000"

	testDB.Exec(nil, utils.ToCmdLine("SETEX", key, ttl, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	assert.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
		return
	}
}

func TestPSetEX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	ttl := "1000000"

	testDB.Exec(nil, utils.ToCmdLine("PSetEx", key, ttl, value))
	actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
	assert.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("PTTL", key))
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000000 {
		t.Errorf("expected int between [0, 10000000], actually %d", intResult.Code)
		return
	}
}

func TestMSet(t *testing.T) {
	testDB.Flush()
	size := 10
	keys := make([]string, size)
	values := make([][]byte, size)
	var args []string
	for i := 0; i < size; i++ {
		keys[i] = utils.RandString(10)
		value := utils.RandString(10)
		values[i] = []byte(value)
		args = append(args, keys[i], value)
	}
	testDB.Exec(nil, utils.ToCmdLine2("MSET", args...))
	actual := testDB.Exec(nil, utils.ToCmdLine2("MGET", keys...))
	expected := protocol.MakeMultiBulkReply(values)
	if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
		t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
	}

	// test mget with wrong type
	key1 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key1, key1))
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("LPush", key2, key2))
	actual = testDB.Exec(nil, utils.ToCmdLine2("MGET", key1, key2))
	arr := actual.(*protocol.MultiBulkReply)
	if string(arr.Args[0]) != key1 {
		t.Error("expected: " + key1 + ", actual: " + string(arr.Args[1]))
	}
	if len(arr.Args[1]) > 0 {
		t.Error("expect null, actual: " + string(arr.Args[0]))
	}
}

func TestIncr(t *testing.T) {
	testDB.Flush()
	size := 10
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCR", key))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBY", key, "-1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(size-i-1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	testDB.Flush()
	key = utils.RandString(10)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBY", key, "1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(i+1), 10)))
		if !utils.BytesEquals(actual.ToBytes(), expected.ToBytes()) {
			t.Error("expected: " + string(expected.ToBytes()) + ", actual: " + string(actual.ToBytes()))
		}
	}
	testDB.Remove(key)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", key, "-1.0"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := -i - 1
		bulk, ok := actual.(*protocol.BulkReply)
		if !ok {
			t.Errorf("expected bulk protocol, actually %s", actual.ToBytes())
			return
		}
		val, err := strconv.ParseFloat(string(bulk.Arg), 10)
		if err != nil {
			t.Error(err)
			return
		}
		if math.Abs(val-float64(expected)) > 1e-4 {
			t.Errorf("expect %d, actual: %d", expected, int(val))
			return
		}
	}
}

func TestDecr(t *testing.T) {
	testDB.Flush()
	size := 10
	key := utils.RandString(10)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("DECR", key))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		assert.AssertBulkReply(t, actual, strconv.Itoa(-i-1))
	}
	testDB.Remove(key)
	for i := 0; i < size; i++ {
		testDB.Exec(nil, utils.ToCmdLine("DECRBY", key, "1"))
		actual := testDB.Exec(nil, utils.ToCmdLine("GET", key))
		expected := -i - 1
		bulk, ok := actual.(*protocol.BulkReply)
		if !ok {
			t.Errorf("expected bulk protocol, actually %s", actual.ToBytes())
			return
		}
		val, err := strconv.ParseFloat(string(bulk.Arg), 10)
		if err != nil {
			t.Error(err)
			return
		}
		if int(val) != expected {
			t.Errorf("expect %d, actual: %d", expected, int(val))
			return
		}
	}
}

func TestGetEX(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	ttl := "1000"

	testDB.Exec(nil, utils.ToCmdLine("SET", key, value))

	// Normal Get
	actual := testDB.Exec(nil, utils.ToCmdLine("GETEX", key))
	assert.AssertBulkReply(t, actual, value)

	// Test GetEX Key EX Seconds
	actual = testDB.Exec(nil, utils.ToCmdLine("GETEX", key, "EX", ttl))
	assert.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000 {
		t.Errorf("expected int between [0, 1000], actually %d", intResult.Code)
		return
	}

	// Test GetEX Key Persist
	actual = testDB.Exec(nil, utils.ToCmdLine("GETEX", key, "PERSIST"))
	assert.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok = actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code != -1 {
		t.Errorf("expected int equals -1, actually %d", intResult.Code)
		return
	}

	// Test GetEX Key NX Milliseconds
	ttl = "1000000"
	actual = testDB.Exec(nil, utils.ToCmdLine("GETEX", key, "PX", ttl))
	assert.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("TTL", key))
	intResult, ok = actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", actual.ToBytes())
		return
	}
	if intResult.Code <= 0 || intResult.Code > 1000000 {
		t.Errorf("expected int between [0, 1000000], actually %d", intResult.Code)
		return
	}
}
func TestGetSet(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("GETSET", key, value))
	_, ok := actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect null bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	value2 := utils.RandString(10)
	actual = testDB.Exec(nil, utils.ToCmdLine("GETSET", key, value2))
	assert.AssertBulkReply(t, actual, value)
	actual = testDB.Exec(nil, utils.ToCmdLine("GET", key))
	assert.AssertBulkReply(t, actual, value2)

	// Test GetDel
	actual = testDB.Exec(nil, utils.ToCmdLine("GETDEL", key))
	assert.AssertBulkReply(t, actual, value2)

	actual = testDB.Exec(nil, utils.ToCmdLine("GETDEL", key))
	_, ok = actual.(*protocol.NullBulkReply)
	if !ok {
		t.Errorf("expect null bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}
}
func TestMSetNX(t *testing.T) {
	testDB.Flush()
	size := 10
	args := make([]string, 0, size*2)
	for i := 0; i < size; i++ {
		str := utils.RandString(10)
		args = append(args, str, str)
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("MSETNX", args...))
	assert.AssertIntReply(t, result, 1)

	result = testDB.Exec(nil, utils.ToCmdLine2("MSETNX", args[0:4]...))
	assert.AssertIntReply(t, result, 0)
}
func TestStrLen(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine2("SET", key, key))

	actual := testDB.Exec(nil, utils.ToCmdLine("StrLen", key))
	size, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect int bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}
	assert.AssertIntReply(t, size, 10)
}

func TestStrLen_KeyNotExist(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)

	actual := testDB.Exec(nil, utils.ToCmdLine("StrLen", key))
	result, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expect null bulk protocol, get: %s", string(actual.ToBytes()))
		return
	}

	assert.AssertIntReply(t, result, 0)
}
