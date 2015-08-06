package blance

import (
	"reflect"
	"testing"
)

func TestFindStateChanges(t *testing.T) {
	tests := []struct {
		begStateIdx     int
		endStateIdx     int
		state           string
		states          []string
		begNodesByState map[string][]string
		endNodesByState map[string][]string
		expected        []string
	}{
		{0, 0, "master",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{"b", "c"},
			},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{"b", "c"},
			},
			nil,
		},
		{1, 2, "master",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{"b", "c"},
			},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{"b", "c"},
			},
			nil,
		},
		{0, 0, "master",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{},
				"replica": []string{"a"},
			},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{},
			},
			nil,
		},
		{1, 2, "master",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{},
				"replica": []string{"a"},
			},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{},
			},
			[]string{"a"},
		},
		{0, 1, "replica",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{},
			},
			map[string][]string{
				"master":  []string{},
				"replica": []string{"a"},
			},
			[]string{"a"},
		},
		{1, 2, "replica",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{},
			},
			map[string][]string{
				"master":  []string{},
				"replica": []string{"a"},
			},
			nil,
		},
		{1, 2, "replica",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{},
				"replica": []string{"a"},
			},
			map[string][]string{
				"master":  []string{},
				"replica": []string{},
			},
			nil,
		},
		{1, 2, "master",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{"b", "c", "d"},
			},
			map[string][]string{
				"master":  []string{"b"},
				"replica": []string{"a", "c", "d"},
			},
			[]string{"b"},
		},
		{1, 2, "master",
			[]string{"master", "replica"},
			map[string][]string{
				"master":  []string{"a"},
				"replica": []string{"b", "c", "d"},
			},
			map[string][]string{
				"master":  []string{"x"},
				"replica": []string{"a", "c", "d"},
			},
			nil,
		},
	}

	for i, test := range tests {
		got := findStateChanges(test.begStateIdx, test.endStateIdx,
			test.state, test.states,
			test.begNodesByState,
			test.endNodesByState)
		if !reflect.DeepEqual(got, test.expected) {
			t.Errorf("i: %d, got: %#v, expected: %#v, test: %#v",
				i, got, test.expected, test)
		}
	}
}

func TestCalcPartitionMoves(t *testing.T) {
	tests := []struct {
		before string
		moves  string
		after  string
	}{
		//  master | replica
		//  -------|--------
		{
			" a",
			"",
			" a",
		},
		{
			" a    | b",
			"",
			" a    | b",
		},
		{
			" a",
			` a +b |
             -a  b |`,
			"    b",
		},
		{
			" a    | b  c",
			` a +b |-b  c
             -a  b |    c
                 b |    c +d`,
			"    b |    c  d",
		},
		{
			" a    |    b",
			` a +b |   -b
             -a  b |+a`,
			"    b | a",
		},
		{
			" a    |    b",
			` a +c |    b
             -a  c |+a  b
                 c | a -b`,
			"    c | a",
		},
		{
			" a    | b",
			` a +c | b
             -a  c | b
                 c | b +d
                 c |-b  d`,
			"    c |    d",
		},
	}

	for i, test := range tests {
		// TODO.
		if test.before == "" {
			t.Errorf("i: %d, test: %#v", i, test)
		}
	}
}
