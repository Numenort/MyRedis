package sortedset

import (
	"testing"
)

func TestRandom(t *testing.T) {
	for i := 0; i < 199; i++ {
		randomLevel()
	}
}
