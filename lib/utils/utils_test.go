package utils // 替换为你实际的包名

import (
	"fmt"
	"testing"
)

// 定义一个测试用例结构体
type RangeTestCase struct {
	start int64
	end   int64
	size  int64
	want  [2]int
}

func TestConvertRange(t *testing.T) {
	tests := []RangeTestCase{
		// 正常范围
		{start: 0, end: 5, size: 10, want: [2]int{0, 6}},
		{start: 2, end: -2, size: 10, want: [2]int{2, 9}},
		{start: -3, end: -1, size: 10, want: [2]int{7, 10}},
		{start: -10, end: -5, size: 10, want: [2]int{0, 6}},

		// 越界情况
		{start: 12, end: 15, size: 10, want: [2]int{-1, -1}},  // start 和 end 都越界
		{start: 8, end: 20, size: 10, want: [2]int{8, 10}},    // end 超出 size
		{start: -11, end: 0, size: 10, want: [2]int{-1, -1}},  // start 太小
		{start: 5, end: 2, size: 10, want: [2]int{-1, -1}},    // start > end
		{start: -1, end: -10, size: 10, want: [2]int{-1, -1}}, // start > end（负数）
		{start: 0, end: -11, size: 10, want: [2]int{-1, -1}},  // end 太小
		{start: 0, end: -1, size: 10, want: [2]int{0, 10}},    // 整个字符串
		{start: 0, end: 0, size: 10, want: [2]int{0, 1}},      // 只取第一个字符
		{start: 9, end: 9, size: 10, want: [2]int{9, 10}},     // 只取最后一个字符
		{start: 0, end: 100, size: 5, want: [2]int{0, 5}},     // end 远大于 size
		{start: -1, end: -1, size: 5, want: [2]int{4, 5}},     // 取最后一个字符
		{start: -5, end: -1, size: 5, want: [2]int{0, 5}},     // 全部元素
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("start=%d,end=%d,size=%d", test.start, test.end, test.size), func(t *testing.T) {
			gotStart, gotEnd := ConvertRange(test.start, test.end, test.size)
			expectedStart, expectedEnd := test.want[0], test.want[1]

			if gotStart != expectedStart || gotEnd != expectedEnd {
				t.Errorf("ConvertRange(%d, %d, %d) = (%d, %d), want (%d, %d)",
					test.start, test.end, test.size,
					gotStart, gotEnd,
					expectedStart, expectedEnd)
			}
		})
	}
}
