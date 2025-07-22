package database

import (
	"myredis/datastruct/sortedset"
	SortedSet "myredis/datastruct/sortedset"
	"myredis/interface/database"
	"myredis/interface/myredis"
	"myredis/protocol"
	"strconv"
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

// func undoZAdd(db *DB, args [][]byte) []CmdLine {
// 	key := string(args[0])
// 	size := (len(args) - 1) / 2
// 	fields := make([]string, size)
// 	for i := 0; i < size; i++ {
// 		fields[i] = string(args[2*i+2])
// 	}
// 	return rollbackZSetFields(db, key, fields...)
// }

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
