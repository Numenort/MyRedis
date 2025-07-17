package set

import (
	"myredis/datastruct/dict"
	"myredis/lib/wildcard"
)

type Set struct {
	dict dict.Dict
}

/* Set 可使用两种低层实现，Simple Dict 和 Concurrent Dict */
func Make(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimple(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func MakeConcurrentSafe(members ...string) *Set {
	set := &Set{
		dict: dict.MakeConcurrent(1),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (s *Set) Add(val string) int {
	return s.dict.Put(val, nil)
}

func (s *Set) Remove(val string) int {
	_, ret := s.dict.Remove(val)
	return ret
}

func (s *Set) Has(val string) bool {
	if s == nil || s.dict == nil {
		return false
	}
	_, exists := s.dict.Get(val)
	return exists
}

func (s *Set) Len() int {
	if s == nil || s.dict == nil {
		return 0
	}
	return s.dict.Len()
}

func (s *Set) ToSlice() []string {
	slice := make([]string, s.Len())
	i := 0
	s.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

func (s *Set) ForEach(consumer func(member string) bool) {
	if s == nil || s.dict == nil {
		return
	}
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

func (s *Set) ShallowCopy() *Set {
	result := Make()
	s.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

func Intersect(sets ...*Set) *Set {
	result := Make()
	if len(sets) == 0 {
		return result
	}

	countMap := make(map[string]int)

	for _, set := range sets {
		set.ForEach(func(member string) bool {
			countMap[member]++
			return true
		})
	}
	for k, v := range countMap {
		if v == len(sets) {
			result.Add(k)
		}
	}
	return result
}

func Union(sets ...*Set) *Set {
	result := Make()
	if len(sets) == 0 {
		return result
	}
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			result.Add(member)
			return true
		})
	}
	return result
}

func Diff(sets ...*Set) *Set {
	if len(sets) == 0 {
		return Make()
	}
	result := sets[0].ShallowCopy()
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			result.Remove(member)
			return true
		})
		if result.Len() == 0 {
			break
		}
	}
	return result
}

func (s *Set) RandomMembers(limit int) []string {
	if s == nil || s.dict == nil {
		return nil
	}
	return s.dict.RandomKeys(limit)
}

func (s *Set) RandomDistinctKeys(limit int) []string {
	return s.dict.RandomDistinctKeys(limit)
}

func (s *Set) SetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	s.ForEach(func(member string) bool {
		if pattern == "*" || matchKey.IsMatch(member) {
			result = append(result, []byte(member))
		}
		return true
	})
	return result, 0
}
