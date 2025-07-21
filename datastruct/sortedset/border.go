package sortedset

/*
	Border 接口：用于表示范围边界，支持多种类型边界：
		ScoreBorder
		LexBorder
		Infinity
*/

type Border interface {
	greater(element *Element) bool
	less(element *Element) bool
	getValue() interface{}
	getExclude() bool
	isIntersected(max Border) bool
}

const (
	scoreNegativeInf int8 = -1
	scorePositiveInf int8 = 1
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)

// Inf: 定义边界是否无穷
//
// value：具体的值
//
// Exclude：开闭区间
type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

func (border *ScoreBorder) greater(elelment *Element) bool {
	value := elelment.Score
	if border.Inf == scoreNegativeInf {
		return false
	} else if border.Inf == scorePositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

func (border *ScoreBorder) less(elelment *Element) bool {
	value := elelment.Score
	if border.Inf == scoreNegativeInf {
		return false
	} else if border.Inf == scorePositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

func (border *ScoreBorder) getValue() interface{} {
	return border.Value
}

func (border *ScoreBorder) getExclude() bool {
	return border.Exclude
}
