package blance

import (
	"testing"
)

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
