package sortedset

import (
	"errors"
	"strconv"
)

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

// 判断 min 的边界是否和 max 有相交
func (border *ScoreBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*ScoreBorder).Value
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

var scorePositiveInfBorder = &ScoreBorder{
	Inf: scorePositiveInf,
}

var scoreNegativeInfBorder = &ScoreBorder{
	Inf: scoreNegativeInf,
}

// 从 redis 命令中解析 ScoreBorder
//
// 例如：inf / -inf / (15 / 16
func ParseScoreBorder(s string) (Border, error) {
	if s == "inf" || s == "+inf" {
		return scorePositiveInfBorder, nil
	}
	if s == "-inf" {
		return scoreNegativeInfBorder, nil
	}

	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}

const (
	lexNegativeInf int8 = '-'
	lexPositiveInf int8 = '+'
)

type LexBorder struct {
	Inf     int8
	Value   string
	Exclude bool
}

// border 的边界是否大于 element
func (border *LexBorder) greater(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return false
	} else if border.Inf == lexPositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

func (border *LexBorder) less(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return true
	} else if border.Inf == lexPositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

func (border *LexBorder) getValue() interface{} {
	return border.Value
}

func (border *LexBorder) getExclude() bool {
	return border.Exclude
}

func (border *LexBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*LexBorder).Value
	return border.Inf == '+' || minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

var lexPositiveInfBorder = &LexBorder{
	Inf: lexPositiveInf,
}

var lexNegativeInfBorder = &LexBorder{
	Inf: lexNegativeInf,
}

// 从 redis 命令中解析 LexBorder
//
// 例如：+ / - / (man / mamba
func ParseLexBorder(s string) (Border, error) {
	if s == "+" {
		return lexPositiveInfBorder, nil
	}
	if s == "-" {
		return lexNegativeInfBorder, nil
	}

	if s[0] == '(' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: true,
		}, nil
	}

	if s[0] == '[' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: false,
		}, nil
	}

	return nil, errors.New("ERR min or max not valid string range item")
}
