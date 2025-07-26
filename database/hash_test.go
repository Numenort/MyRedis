package database

import (
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
