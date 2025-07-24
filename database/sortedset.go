package database

import (
	"math"
	"myredis/datastruct/sortedset"
	SortedSet "myredis/datastruct/sortedset"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/protocol"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2/ext/tryfunc"
)

func (db *DB) getAsSortedSet(key string) (*sortedset.SortedSet, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*sortedset.SortedSet)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return sortedSet, nil
}

func (db *DB) getOrInitSortedSet(key string) (sortedSet *sortedset.SortedSet, inited bool, errReply protocol.ErrorReply) {
	sortedSet, errReply = db.getAsSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if sortedSet == nil {
		sortedSet = SortedSet.Make()
		db.PutEntity(key, &database.DataEntity{
			Data: sortedSet,
		})
		inited = true
	}
	return sortedSet, inited, nil
}

func execZAdd(db *DB, args [][]byte) myredis.Reply {
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	elements := make([]*SortedSet.Element, size)
	for i := 0; i < size; i++ {
		rawScore := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(rawScore), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		elements[i] = &SortedSet.Element{
			Member: member,
			Score:  score,
		}
	}

	sortedset, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	i := 0
	for _, element := range elements {
		if sortedset.Add(element.Member, element.Score) {
			i++
		}
	}
	return protocol.MakeIntReply(int64(i))
}

func undoZAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+2])
	}
	return rollbackZSetFields(db, key, fields...)
}

func execZScore(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	element, exists := sortedSet.Get(member)
	if !exists {
		return protocol.MakeNullBulkReply()
	}

	value := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return protocol.MakeBulkReply([]byte(value))
}

func execZRank(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	rank := sortedSet.GetRank(member, false)
	if rank < 0 {
		return protocol.MakeNullBulkReply()
	}

	return protocol.MakeIntReply(rank)
}

func execZRevRank(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	rank := sortedSet.GetRank(member, true)
	if rank < 0 {
		return protocol.MakeNullBulkReply()
	}

	return protocol.MakeIntReply(rank)
}

func execZCard(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(sortedSet.Len())
}

func range0(db *DB, key string, start int64, end int64, withScores bool, desc bool) myredis.Reply {
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	size := sortedSet.Len()
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = start + size
	} else if start >= size {
		return protocol.MakeEmptyMultiBulkReply()
	}

	if end < -1*size {
		end = 0
	} else if end < 0 {
		end = end + size + 1
	} else if end < size {
		end = end + 1
	} else {
		end = size
	}
	if end < start {
		end = start
	}

	slice := sortedSet.RangeByRank(start, end, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			score := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(score)
			i++
		}
		return protocol.MakeMultiBulkReply(result)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZRange(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, end, withScores, false)
}

func execZRevRange(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeErrReply("syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, end, withScores, true)
}

func execZCount(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet != nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(sortedSet.RangeCount(min, max))
}

func rangeByScore0(db *DB, key string, min SortedSet.Border, max SortedSet.Border, offset int64, limit int64, withScores bool, desc bool) myredis.Reply {
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	slice := sortedSet.Range(min, max, offset, limit, desc)
	if withScores {
		result := make([][]byte, 2*len(slice))
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			score := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(score)
			i++
		}
		return protocol.MakeMultiBulkReply(result)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZRangeByScore(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
	}

	key := string(args[0])
	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	withScores := false
	var offset int64 = 0
	var limit int64 = -1

	if len(args) > 3 {
		for i := 3; i < len(args); {
			v := string(args[i])
			if strings.ToUpper(v) == "WITHSCORES" {
				withScores = true
			} else if strings.ToUpper(v) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}

func execZRevRangeByScore(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
	}

	key := string(args[0])
	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	withScores := false
	var offset int64 = 0
	var limit int64 = -1

	if len(args) > 3 {
		for i := 3; i < len(args); {
			v := string(args[i])
			if strings.ToUpper(v) == "WITHSCORES" {
				withScores = true
			} else if strings.ToUpper(v) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

func execZRemRangeByScore(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zremrangebyscore' command")
	}
	key := string(args[0])

	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	removed := sortedset.RemoveRange(min, max)
	return protocol.MakeIntReply(removed)
}

func execZRemRangeByRank(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zremrangebyrank' command")
	}
	key := string(args[0])

	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeMultiBulkReply()
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	size := sortedset.Len()
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return protocol.MakeIntReply(0)
	}
	if end < -1*size {
		end = 0
	} else if end < 0 {
		end = end + size + 1
	} else if end < size {
		end = end + 1
	} else {
		end = size
	}
	if end < start {
		end = start
	}

	removed := sortedset.RemoveByRank(start, end)
	return protocol.MakeIntReply(removed)
}

func execZPopMin(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
	}

	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeMultiBulkReply()
	}
	removed := sortedset.PopMin(count)
	result := make([][]byte, len(removed)*2)
	i := 0
	for _, element := range removed {
		score := strconv.FormatFloat(element.Score, 'f', -1, 64)
		result[i] = []byte(element.Member)
		i++
		result[i] = []byte(score)
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZRem(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	fields := make([]string, len(args)-1)
	for i, val := range args[1:] {
		fields[i] = string(val)
	}

	var removed int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			removed++
		}
	}
	return protocol.MakeIntReply(removed)
}

func undoZRem(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	filed := make([]string, len(args)-1)
	for i, val := range args[1:] {
		filed[i] = string(val)
	}
	return rollbackZSetFields(db, key, filed...)
}

func execZIncrBy(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	rawDelta := string(args[1])
	member := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	// 检查对应的 field 是否存在
	element, exists := sortedSet.Get(member)
	if !exists {
		sortedSet.Add(member, delta)
		return protocol.MakeBulkReply(args[1])
	}
	score := element.Score + delta
	sortedSet.Add(member, score)
	bytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))
	return protocol.MakeBulkReply(bytes)
}

func undoZIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	member := string(args[1])
	return rollbackZSetFields(db, key, member)
}

func execZLexCount(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeIntReply(0)
	}

	min_S, max_S := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(min_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(max_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	count := sortedset.RangeCount(min, max)
	return protocol.MakeIntReply(count)
}

func execZRangeByLex(db *DB, args [][]byte) myredis.Reply {
	size := len(args)
	if size > 3 && strings.ToUpper(string(args[3])) != "LIMIT" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	if size != 3 && size != 6 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := string(args[0])
	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeIntReply(0)
	}

	min_S, max_S := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(min_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(max_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// 解析额外命令: offset / limit
	offset := int64(0)
	limit := int64(math.MaxInt64)
	if size > 3 {
		var err error
		offset, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count, err := strconv.ParseInt(string(args[5]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if count >= 0 {
			limit = count
		}
	}

	elements := sortedset.Range(min, max, offset, limit, false)
	result := make([][]byte, 0, len(elements))
	for _, element := range elements {
		result = append(result, []byte(element.Member))
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZRevRangeByLex(db *DB, args [][]byte) myredis.Reply {
	size := len(args)
	if size > 3 && strings.ToUpper(string(args[3])) != "LIMIT" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	if size != 3 && size != 6 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := string(args[0])
	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeIntReply(0)
	}

	min_S, max_S := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(min_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(max_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// 解析额外命令: offset / limit
	offset := int64(0)
	limit := int64(math.MaxInt64)
	if size > 3 {
		var err error
		offset, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count, err := strconv.ParseInt(string(args[5]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if count >= 0 {
			limit = count
		}
	}

	elements := sortedset.Range(min, max, offset, limit, true)
	result := make([][]byte, 0, len(elements))
	for _, element := range elements {
		result = append(result, []byte(element.Member))
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZRemRangeByLex(db *DB, args [][]byte) myredis.Reply {
	size := len(args)
	if size != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := string(args[0])
	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeIntReply(0)
	}

	min_S, max_S := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(min_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(max_S)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	count := sortedset.RemoveRange(min, max)
	return protocol.MakeIntReply(count)
}

func execZScan(db *DB, args [][]byte) myredis.Reply {
	var count int = 10
	var pattern string = "*"
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "count" {
				tempCount, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return protocol.MakeSyntaxErrReply()
				}
				count = tempCount
				i++
			} else if arg == "match" {
				pattern = string(args[i+1])
				i++
			} else {
				return protocol.MakeSyntaxErrReply()
			}
		}
	}
	key := string(args[0])
	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid cursor")
	}

	keysReply, nextCursor := sortedset.ZSetScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("Invalid argument")
	}

	result := make([]myredis.Reply, 2)
	result[0] = protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10)))
	result[1] = protocol.MakeMultiBulkReply(keysReply)

	return protocol.MakeMultiRawReply(result)
}
