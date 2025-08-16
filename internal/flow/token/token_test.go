package token

import (
	"fmt"
	"testing"
)

func testSplitToken(t *testing.T, val Value, count int) {
	vals := val.Split(count)
	out := vals[0]
	for _, tok := range vals[1:] {
		out = out.Merge(tok)
	}
	fmt.Println(val, out)
	if !out.Equal(val) {
		t.Errorf("not equal: %v %v", val, out)
	}
}

func TestTokenValue(t *testing.T) {
	testSplitToken(t, Zero(), 1)
	testSplitToken(t, Zero(), 5)

	testSplitToken(t, Rand(), 1)
	testSplitToken(t, Rand(), 5)

	// todo: assert these panic?
	// testSplitToken(t, Rand(), -1)
	// testSplitToken(t, Rand(), 0)
}
