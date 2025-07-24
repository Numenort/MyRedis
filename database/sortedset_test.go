package database

import (
	"math/rand"
	"myredis/lib/utils"
	"myredis/protocol/assert"
	"strconv"
	"testing"
)

func TestZAdd(t *testing.T) {
	testDB.Flush()
	size := 100

	// add new members
	key := utils.RandString(10)
	members := make([]string, size)
	scores := make([]float64, size)
	setArgs := []string{key}
	for i := 0; i < size; i++ {
		members[i] = utils.RandString(10)
		scores[i] = rand.Float64()
		setArgs = append(setArgs, strconv.FormatFloat(scores[i], 'f', -1, 64), members[i])
	}
	result := testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))
	assert.AssertIntReply(t, result, size)

	// test zscore and zrank
	for i, member := range members {
		result = testDB.Exec(nil, utils.ToCmdLine("ZScore", key, member))
		score := strconv.FormatFloat(scores[i], 'f', -1, 64)
		assert.AssertBulkReply(t, result, score)
	}

	// test zcard
	result = testDB.Exec(nil, utils.ToCmdLine("zcard", key))
	assert.AssertIntReply(t, result, size)

	// update members
	setArgs = []string{key}
	for i := 0; i < size; i++ {
		scores[i] = rand.Float64() + 100
		setArgs = append(setArgs, strconv.FormatFloat(scores[i], 'f', -1, 64), members[i])
	}
	result = testDB.Exec(nil, utils.ToCmdLine2("zadd", setArgs...))
	assert.AssertIntReply(t, result, 0) // return number of new members

	// test updated score
	for i, member := range members {
		result = testDB.Exec(nil, utils.ToCmdLine("zscore", key, member))
		score := strconv.FormatFloat(scores[i], 'f', -1, 64)
		assert.AssertBulkReply(t, result, score)
	}
}
