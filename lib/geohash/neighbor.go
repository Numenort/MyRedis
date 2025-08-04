/*
提供基于 GeoHash 的地理编码与邻近区域计算功能。

主要实现以下功能：
  - 计算两点间的球面距离（Haversine 公式）
  - 根据搜索半径自动估算合适的 GeoHash 精度
  - 将经纬度编码为 GeoHash 并获取其数值范围
  - 获取指定坐标在给定半径内的 3x3 邻接 GeoHash 区域范围
*/

package geohash

import "math"

const (
	dr          = math.Pi / 180.0
	earthRadius = 6372797.560856 // 地球半径
	mercatorMax = 20037726.37    // Web 墨卡托投影中的最大坐标值
	mercatorMin = -20037726.37   // 最小值
)

// deg2rad：将角度转换为弧度
func deg2rad(angle float64) float64 {
	return angle * dr
}

// deg2rad：将弧度转换为角度
func rad2deg(angle float64) float64 {
	return angle / dr
}

// Distance: 计算两点间球面距离 (使用 Haversine 公式)
func Distance(latitude1, longitude1, latitude2, longitude2 float64) float64 {
	radLat1 := deg2rad(latitude1)
	radLat2 := deg2rad(latitude2)
	a := radLat1 - radLat2
	b := deg2rad(longitude1) - deg2rad(longitude2)
	return 2 * earthRadius * math.Asin(math.Sqrt(math.Pow(math.Sin(a/2), 2)+
		math.Cos(radLat1)*math.Cos(radLat2)*math.Pow(math.Sin(b/2), 2)))
}

// estimatePrecisionByRadius 根据搜索半径（米）估算合适的 GeoHash 位数
func estimatePrecisionByRadius(radisMeters float64, latitude float64) uint {
	// 最大精度
	if radisMeters == 0 {
		return defaultBitSize - 1
	}
	var level uint = 1
	// 从搜索半径开始，直到 Web 墨卡托投影最大值
	for radisMeters < mercatorMax {
		radisMeters *= 2
		level++
	}
	level -= 2
	if latitude > 66 || latitude < -66 {
		level--
	}
	if latitude > 80 || latitude < -80 {
		level--
	}

	if level < 1 {
		level = 1
	}
	if level > 32 {
		level = 32
	}
	return level*2 - 1
}

// toRange 将 GeoHash 前缀（字节形式）转换为对应的 uint64 数值范围 [min, max)
func toRange(scope []byte, precision uint) [2]uint64 {
	lower := ToInt(scope) // 该 geohas 前缀开头的最小值
	// 区间跨度，对应精度的块大小
	radius := uint64(1 << (64 - precision)) // (64 - precision)：还剩下多少位是“可变的”
	upper := lower + radius
	return [2]uint64{lower, upper}
}

func getValidLat(latitude float64) float64 {
	if latitude > 90 {
		return 90
	}
	if latitude < -90 {
		return -90
	}
	return latitude
}

func getValidLng(longitude float64) float64 {
	if longitude > 180 {
		return -360 + longitude
	}
	if longitude < -180 {
		return 360 + longitude
	}
	return longitude
}

// GetNeighbours 返回以指定坐标为中心、在给定半径范围内的 9 个相邻 GeoHash 区块的查询范围。
// 返回的每个范围均为左闭右开区间 [min, max)。
//
// 参数：
//
//	latitude: 纬度（-90 ~ 90）
//	longitude: 经度（-180 ~ 180）
//	radiusMeter: 搜索半径（单位：米）
//
// 返回值：
//
//	[][2]uint64: 9 个 GeoHash 范围，顺序为从左上到右下的 3x3 网格：
//	  0: 左上  1: 上中  2: 右上
//	  3: 左中  4: 中心  5: 右中
//	  6: 左下  7: 下中  8: 右下
func GetNeighbours(latitude, longitude, radiusMeter float64) [][2]uint64 {
	// 选择 Geohash 精度
	level := estimatePrecisionByRadius(radiusMeter, latitude)

	center, box := encode(latitude, longitude, level)
	width := box[0][1] - box[0][0]
	height := box[1][1] - box[1][0]

	centerLng := (box[0][1] + box[0][0]) / 2
	centerLat := (box[1][1] + box[1][0]) / 2
	maxLat := getValidLat(centerLat + height) // 纬度
	minLat := getValidLat(centerLat - height)

	maxLng := getValidLng(centerLng + width) // 经度
	minLng := getValidLng(centerLng - width)

	// 3 * 3的边界
	var result [9][2]uint64
	leftUpper, _ := encode(maxLat, minLng, level)
	result[0] = toRange(leftUpper, level)
	upper, _ := encode(maxLat, centerLng, level)
	result[1] = toRange(upper, level)
	rightUpper, _ := encode(maxLat, maxLng, level)
	result[2] = toRange(rightUpper, level)
	left, _ := encode(centerLat, minLng, level)
	result[3] = toRange(left, level)
	result[4] = toRange(center, level)
	right, _ := encode(centerLat, maxLng, level)
	result[5] = toRange(right, level)
	leftDown, _ := encode(minLat, minLng, level)
	result[6] = toRange(leftDown, level)
	down, _ := encode(minLat, centerLng, level)
	result[7] = toRange(down, level)
	rightDown, _ := encode(minLat, maxLng, level)
	result[8] = toRange(rightDown, level)

	return result[:] // 返回切片
}
