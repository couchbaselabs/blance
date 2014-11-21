package blance

import (
	"reflect"
	"testing"
)

func TestStringsToMap(t *testing.T) {
	s := []string{}
	m := StringsToMap(s)
	if m == nil || len(m) != 0 {
		t.Errorf("expected StringsToMap to work on empty array")
	}
	m = StringsToMap([]string{"a"})
	if m == nil || !reflect.DeepEqual(m, map[string]bool{"a": true}) {
		t.Errorf("expected single string arr to work")
	}
	m = StringsToMap([]string{"a", "b", "a"})
	if m == nil || !reflect.DeepEqual(m, map[string]bool{"a": true, "b": true}) {
		t.Errorf("expected 3 string arr to work with dupe removal")
	}
}
