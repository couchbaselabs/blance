package blance

import (
	"testing"
)

func TestStringsToMap(t *testing.T) {
	s := []string{}
	m := StringsToMap(s)
	if m == nil || len(m) != 0 {
		t.Errorf("expected StringsToMap to work on empty array")
	}
}
