package utils

import (
	"math/rand"
	"time"
)

// ConvertRange 将 Redis 风格的索引范围转换为 Go 切片可用的索引范围。
//
// Redis 索引支持负数：
//
//	-1 表示最后一个元素，-2 表示倒数第二个元素，以此类推
//
// 区间是闭区间 [start, end]，即包含 start 和 end 本身
//
// Go 切片使用左闭右开区间 [start, end)，不支持负数索引
//
// 参数说明：
//
//	start: Redis 起始索引（可为负）
//	end:   Redis 结束索引（可为负）
//	size:  当前字符串或列表的长度
//
// 返回值：
//
//	(int, int)：Go 风格的起始和结束索引（左闭右开）
//	如果范围无效，返回 (-1, -1)
func ConvertRange(start int64, end int64, size int64) (int, int) {
	if start < -size {
		return -1, -1
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return -1, -1
	}

	if end < -size {
		return -1, -1
	} else if end < 0 {
		end = size + end + 1
	} else if end < size {
		end = end + 1
	} else {
		end = size
	}

	if start > end {
		return -1, -1
	}
	return int(start), int(end)
}

// 将 string 类型的命令转为 [][]byte 类型（即 CmdLine)
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// 将 command 和 args 命令转为 CmdLine 类型
func ToCmdLine2(command string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(command)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
}

// 检查两个任意类型的变量是否相同
func Equals(a interface{}, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// 检查两个 []byte 类型的变量是否相同
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}
