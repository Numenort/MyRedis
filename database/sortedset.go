package database

import (
	"math"
	"myredis/datastruct/sortedset"
	SortedSet "myredis/datastruct/sortedset"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/lib/utils"
	"myredis/protocol"
	"strconv"
	"strings"
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

// 处理 ZADD 命令，向有序集合添加一个或多个成员及其分数。示例：ZADD myzset 100 "member1" 200 "member2"
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
	db.addAof(utils.ToCmdLine3("zadd", args...))
	return protocol.MakeIntReply(int64(i))
}

// 为 ZADD 命令生成回滚操作
func undoZAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+2])
	}
	return rollbackZSetFields(db, key, fields...)
}

// 处理 ZSCORE 命令，获取有序集合中指定成员的分数。示例：ZSCORE myzset "member1"
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

// 处理 ZRANK 命令，获取有序集合中指定成员的排名（分数从低到高，0-based）。示例：ZRANK myzset "member1"
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

// 处理 ZREVRANK 命令，获取有序集合中指定成员的逆序排名（分数从高到低，0-based）。示例：ZREVRANK myzset "member1"
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

// 处理 ZCARD 命令，获取有序集合的成员数量。示例：ZCARD myzset
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

// 处理 ZRANGE 命令，通过索引范围获取有序集合的成员。示例：ZRANGE myzset 0 -1 WITHSCORES
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

// 处理 ZREVRANGE 命令，通过索引范围逆序获取有序集合的成员。示例：ZREVRANGE myzset 0 10
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

// 处理 ZCOUNT 命令，统计有序集合中指定分数范围内的成员数量。示例：ZCOUNT myzset 0 100
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
	if sortedSet == nil {
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

// 处理 ZRANGEBYSCORE 命令，通过分数范围获取有序集合的成员。示例：ZRANGEBYSCORE myzset (10 100 LIMIT 0 5
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
				i++
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

// 处理 ZREVRANGEBYSCORE 命令，通过分数范围逆序获取有序集合的成员。示例：ZREVRANGEBYSCORE myzset 100 0 WITHSCORES
func execZRevRangeByScore(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
	}

	key := string(args[0])
	min, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseScoreBorder(string(args[1]))
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
				i++
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

// 处理 ZREMRANGEBYSCORE 命令，通过分数范围移除有序集合中的成员。示例：ZREMRANGEBYSCORE myzset -inf (100
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
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("zremrangebyscore", args...))
	}
	return protocol.MakeIntReply(removed)
}

// 处理 ZREMRANGEBYRANK 命令，通过排名范围移除有序集合中的成员。示例：ZREMRANGEBYRANK myzset 0 5
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
		return protocol.MakeEmptyMultiBulkReply()
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
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("zremrangebyrank", args...))
	}
	return protocol.MakeIntReply(removed)
}

// 处理 ZPOPMIN 命令，移除并返回有序集合中分数最小的成员。示例：ZPOPMIN myzset 2
func execZPopMin(db *DB, args [][]byte) myredis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
	}

	sortedset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedset == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	removed := sortedset.PopMin(count)
	result := make([][]byte, len(removed)*2)
	i := 0
	for _, element := range removed {
		score := strconv.FormatFloat(element.Score, 'f', -1, 64)
		result[i] = []byte(element.Member)
		i++
		result[i] = []byte(score)
		i++
	}
	if len(removed) > 0 {
		db.addAof(utils.ToCmdLine3("zpopmin", args...))
	}
	return protocol.MakeMultiBulkReply(result)
}

// 处理 ZREM 命令，从有序集合中移除一个或多个成员。示例：ZREM myzset "member1" "member2"
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
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("zrem", args...))
	}
	return protocol.MakeIntReply(removed)
}

// 为 ZREM 命令生成回滚操作
func undoZRem(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	filed := make([]string, len(args)-1)
	for i, val := range args[1:] {
		filed[i] = string(val)
	}
	return rollbackZSetFields(db, key, filed...)
}

// 处理 ZINCRBY 命令，增加有序集合中指定成员的分数。示例：ZINCRBY myzset 1.5 "member1"
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
		db.addAof(utils.ToCmdLine3("zincrby", args...))
		return protocol.MakeBulkReply(args[1])
	}
	score := element.Score + delta
	sortedSet.Add(member, score)
	bytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))
	db.addAof(utils.ToCmdLine3("zincrby", args...))
	return protocol.MakeBulkReply(bytes)
}

// 为 ZINCRBY 命令生成回滚操作
func undoZIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	member := string(args[1])
	return rollbackZSetFields(db, key, member)
}

// 处理 ZLEXCOUNT 命令，统计有序集合中指定字典序范围内的成员数量。示例：ZLEXCOUNT myzset [a [c
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

// 处理 ZRANGEBYLEX 命令，通过字典序范围获取有序集合的成员。示例：ZRANGEBYLEX myzset [a [c LIMIT 0 10
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

// 处理 ZREVRANGEBYLEX 命令，通过字典序范围逆序获取有序集合的成员。示例：ZREVRANGEBYLEX myzset [c [a
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

	min_S, max_S := string(args[2]), string(args[1])
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

// 处理 ZREMRANGEBYLEX 命令，通过字典序范围移除有序集合中的成员。示例：ZREMRANGEBYLEX myzset [a (c
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

// 处理 ZSCAN 命令，迭代有序集合中的成员。示例：ZSCAN myzset 0 MATCH user:* COUNT 100
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

func init() {
	registerCommand("ZAdd", execZAdd, writeFirstKey, undoZAdd, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRem", execZRem, writeFirstKey, undoZRem, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRemRangeByScore", execZRemRangeByScore, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZRemRangeByRank", execZRemRangeByRank, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZIncrBy", execZIncrBy, writeFirstKey, undoZIncr, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ZPopMin", execZPopMin, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)

	registerCommand("ZScore", execZScore, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRank", execZRank, readFirstKey, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRevRank", execZRevRank, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZCount", execZCount, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZCard", execZCard, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRange", execZRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRevRange", execZRevRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRangeByScore", execZRangeByScore, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRevRangeByScore", execZRevRangeByScore, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)

	registerCommand("ZLexCount", execZLexCount, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRangeByLex", execZRangeByLex, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRemRangeByLex", execZRemRangeByLex, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZRevRangeByLex", execZRevRangeByLex, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZScan", execZScan, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
