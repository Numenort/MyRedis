package database

import (
	"fmt"
	"myredis/datastruct/sortedset"
	"myredis/interface/myredis"
	"myredis/lib/geohash"
	"myredis/protocol"
	"strconv"
	"strings"
)

// execGeoAdd: 将一个或多个地理位置（经度、纬度、成员名）添加到指定的 SortedSet 中。
// 位置以 GeoHash 编码存储为 Score，实现地理索引。
// 调用示例: GEOADD locations 13.361389 38.115556 "Palermo" 15.087267 37.502669 "Catania"
// 返回值: 成功添加的新成员数量。
func execGeoAdd(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 4 || len(args)%3 != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geoadd' command")
	}
	key := string(args[0])
	size := (len(args) - 1) / 3
	elements := make([]*sortedset.Element, size)
	// 添加到节点
	for i := 0; i < size; i++ {
		lngStr := string(args[i*3+1])
		latStr := string(args[i*3+2])
		lng, err := strconv.ParseFloat(lngStr, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a vaild float")
		}
		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a vaild float")
		}
		if lat < -90 || lat > 90 || lng < -180 || lng > 180 {
			return protocol.MakeErrReply(fmt.Sprintf("ERR invalid longitude,latitude pair %s,%s", latStr, lngStr))
		}
		code := float64(geohash.Encode(lat, lng))
		elements[i] = &sortedset.Element{
			Member: string(args[3*i+3]),
			Score:  code,
		}
	}
	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}
	i := 0
	for _, element := range elements {
		if sortedSet.Add(element.Member, element.Score) {
			i++
		}
	}
	return protocol.MakeIntReply(int64(i))
}

func undoGeoAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 3
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[i*3+3])
	}
	return rollbackZSetFields(db, key, fields...)
}

// execGeoPos: 获取一个或多个成员的经纬度坐标。
// 成员必须存在于指定的 SortedSet 中。
// 调用示例: GEOPOS locations "Palermo" "Catania"
// 返回值: 每个成员对应的 [经度, 纬度] 数组，不存在则返回 nil。
func execGeoPos(db *DB, args [][]byte) myredis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geopos' command")
	}
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeNullBulkReply()
	}

	positions := make([]myredis.Reply, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		member := string(args[i+1])
		element, exists := sortedSet.Get(member)
		if !exists {
			positions[i] = protocol.MakeEmptyMultiBulkReply()
			continue
		}
		lat, lng := geohash.Decode(uint64(element.Score))
		latStr := strconv.FormatFloat(lat, 'f', -1, 64)
		lngStr := strconv.FormatFloat(lng, 'f', -1, 64)
		positions[i] = protocol.MakeMultiBulkReply(
			[][]byte{
				[]byte(lngStr),
				[]byte(latStr),
			},
		)
	}
	return protocol.MakeMultiRawReply(positions)
}

// execGeoDist: 计算两个成员之间的地理距离，默认单位为米（m），支持千米（km）。
// 两个成员都必须存在于同一 SortedSet 中。
// 调用示例: GEODIST locations "Palermo" "Catania" km
// 返回值: 距离数值字符串（如 "166274.1515"），任一成员不存在则返回 nil。
func execGeoDist(db *DB, args [][]byte) myredis.Reply {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geodist' command")
	}
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeNullBulkReply()
	}
	positions := make([][]float64, 2)
	for i := 1; i < 3; i++ {
		member := string(args[i])
		element, exists := sortedSet.Get(member)
		if !exists {
			return protocol.MakeNullBulkReply()
		}
		// 得到经纬度
		lat, lng := geohash.Decode(uint64(element.Score))
		positions[i-1] = []float64{lat, lng}
	}
	distUnit := "m"
	if len(args) == 4 {
		distUnit = strings.ToLower(string(args[3]))
	}
	dist := geohash.Distance(positions[0][0], positions[0][1], positions[1][0], positions[1][1])
	var distStr string
	if distUnit == "m" {
		distStr = strconv.FormatFloat(dist, 'f', -1, 64)
		return protocol.MakeBulkReply([]byte(distStr))
	} else if distUnit == "km" {
		distStr = strconv.FormatFloat(dist/1000, 'f', -1, 64)
		return protocol.MakeBulkReply([]byte(distStr))
	}
	return protocol.MakeErrReply("ERR unsupported unit provided. please use m, km")
}
