package idgenerator

import "testing"

func TestMGenerator(t *testing.T) {
	idGen := MakeIDGenerator("a")
	ids := make(map[int64]struct{})
	size := 1000000
	for i := 0; i < size; i++ {
		id, err := idGen.NextID()
		if err != nil {
			t.Error(err)
		} else {
			_, ok := ids[id]
			if ok {
				t.Errorf("duplicated id: %d, time: %d, seq: %d", id, idGen.lastStamp, idGen.sequence)
			}
			ids[id] = struct{}{}
		}

	}
}
