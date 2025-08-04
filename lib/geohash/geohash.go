/*
Package geohash 提供经纬度的 GeoHash 编码与解码功能。

支持将坐标编码为 uint64、字节数组或 Base32 字符串，
并可通过解码还原位置或获取对应的地理包围盒。
使用 Z-order 曲线实现，用于空间索引和邻近查询。
*/
package geohash

import (
	"encoding/base32"
	"encoding/binary"
)

// 用于 decode 逐位提取字节中的 bit
var bits = []uint8{128, 64, 32, 16, 8, 4, 2, 1}
var enc = base32.NewEncoding("0123456789bcdefghjkmnpqrstuvwxyz").WithPadding(base32.NoPadding)

const defaultBitSize = 64

// 输入：纬度、经度、编码总位数
// 输出：
//   - []byte：二进制形式的 GeoHash（大端序）
//   - [2][2]float64：该哈希对应的地理边界框（bounding box）
func encode(latitude, longitude float64, bitSize uint) ([]byte, [2][2]float64) {
	box := [2][2]float64{
		{-180, 180}, // 经度范围
		{-90, 90},   // 纬度范围
	}
	pos := [2]float64{longitude, latitude}

	// 计算需要多少字节存储 bitSize 位数据
	hashLen := bitSize >> 3 // bitSize / 8
	if bitSize&7 > 0 {
		// 向上取整
		hashLen++
	}
	hash := make([]byte, hashLen)
	// 当前精度层级为 0
	var level uint = 0
	for level < bitSize {
		// 二分查找，交替处理经度和纬度，得到最接近的范围框
		for direction, val := range pos {
			mid := (box[direction][0] + box[direction][1]) / 2
			if val < mid {
				box[direction][1] = mid
			} else {
				box[direction][0] = mid
				// 对应比特位设置为 1
				hash[level>>3] |= 1 << (7 - level&7) // level&7 即 precision%8，7-即反转顺序
			}
			level++
			if level == bitSize {
				break
			}
		}
	}
	return hash, box
}

// Encode 将经纬度转换为 uint64 类型的 GeoHash 码
func Encode(latitude, longitude float64) uint64 {
	geohash, _ := encode(latitude, longitude, defaultBitSize)
	// 将字节切片按大端序转为 uint64
	return binary.BigEndian.Uint64(geohash)
}

// 输入：GeoHash 的字节表示
// 返回：[[min_lng, max_lng], [min_lat, max_lat]] box
func decode(hash []byte) [][]float64 {
	box := [][]float64{
		{-180, 180},
		{-90, 90},
	}
	direction := 0
	for i := 0; i < len(hash); i++ {
		code := hash[i]
		for j := 0; j < len(bits); j++ {
			mid := (box[direction][0] + box[direction][1]) / 2
			mask := bits[j]
			if mask&code > 0 {
				box[direction][0] = mid
			} else {
				box[direction][1] = mid
			}
			direction = (direction + 1) % 2
		}
	}
	return box
}

// Decode 将 uint64 GeoHash 码解码为经纬度（取包围盒中心点）
func Decode(code uint64) (float64, float64) {
	hash := make([]byte, 8)
	binary.BigEndian.PutUint64(hash, code)
	box := decode(hash)
	longitude := float64(box[0][0]+box[0][1]) / 2
	latitude := float64(box[1][0]+box[1][1]) / 2
	return latitude, longitude
}

// ToString 将字节形式的 GeoHash 编码为 Base32 字符串
func ToString(geohash []byte) string {
	return enc.EncodeToString(geohash)
}

// ToInt 将字节形式的 GeoHash 转为 uint64 整数
// 如果输入不足 8 字节，高位补 0
func ToInt(geohash []byte) uint64 {
	if len(geohash) < 8 {
		buf := make([]byte, 8)
		copy(buf, geohash)
		return binary.BigEndian.Uint64(buf)
	}
	return binary.BigEndian.Uint64(geohash)
}

// FromInt 将 uint64 GeoHash 码转为字节切片（大端序）
func FromInt(code uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, code)
	return buf
}
