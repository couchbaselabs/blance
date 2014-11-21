package blance

import (
	"reflect"
	"sort"
	"testing"
)

func TestflattenNodesByState(t *testing.T) {
	tests := []struct {
		a   map[string][]string
		exp []string
	}{
		{map[string][]string{},
			[]string{}},
		{map[string][]string{"master": []string{}},
			[]string{}},
		{map[string][]string{"master": []string{"a"}},
			[]string{"a"}},
		{map[string][]string{"master": []string{"a", "b"}},
			[]string{"a", "b"}},
		{map[string][]string{
			"master": []string{"a", "b"},
			"slave":  []string{"c"},
		}, []string{"a", "b", "c"}},
		{map[string][]string{
			"master": []string{"a", "b"},
			"slave":  []string{},
		}, []string{"a", "b"}},
	}
	for i, c := range tests {
		r := flattenNodesByState(c.a)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, a: %#v, exp: %#v, got: %#v",
				i, c.a, c.exp, r)
		}
	}
}

func TestRemoveNodesFromNodesByState(t *testing.T) {
	tests := []struct {
		nodesByState map[string][]string
		removeNodes  []string
		exp          map[string][]string
	}{
		{map[string][]string{"master": []string{"a", "b"}},
			[]string{"a", "b"},
			map[string][]string{"master": []string{}},
		},
		{map[string][]string{"master": []string{"a", "b"}},
			[]string{"b", "c"},
			map[string][]string{"master": []string{"a"}},
		},
		{map[string][]string{"master": []string{"a", "b"}},
			[]string{"a", "c"},
			map[string][]string{"master": []string{"b"}},
		},
		{map[string][]string{"master": []string{"a", "b"}},
			[]string{},
			map[string][]string{"master": []string{"a", "b"}},
		},
		{
			map[string][]string{
				"master": []string{"a", "b"},
				"slave":  []string{"c"},
			},
			[]string{},
			map[string][]string{
				"master": []string{"a", "b"},
				"slave":  []string{"c"},
			},
		},
		{
			map[string][]string{
				"master": []string{"a", "b"},
				"slave":  []string{"c"},
			},
			[]string{"a"},
			map[string][]string{
				"master": []string{"b"},
				"slave":  []string{"c"},
			},
		},
		{
			map[string][]string{
				"master": []string{"a", "b"},
				"slave":  []string{"c"},
			},
			[]string{"a", "c"},
			map[string][]string{
				"master": []string{"b"},
				"slave":  []string{},
			},
		},
	}
	for i, c := range tests {
		r := removeNodesFromNodesByState(c.nodesByState, c.removeNodes, nil)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, nodesByState: %#v, removeNodes: %#v, exp: %#v, got: %#v",
				i, c.nodesByState, c.removeNodes, c.exp, r)
		}
	}
}

func TestStateNameSorter(t *testing.T) {
	tests := []struct {
		m   PartitionModel
		s   []string
		exp []string
	}{
		{
			PartitionModel{
				"master": &PartitionModelState{Priority: 0},
				"slave":  &PartitionModelState{Priority: 1},
			},
			[]string{},
			[]string{},
		},
		{
			PartitionModel{
				"master": &PartitionModelState{Priority: 0},
				"slave":  &PartitionModelState{Priority: 1},
			},
			[]string{"master", "slave"},
			[]string{"master", "slave"},
		},
		{
			PartitionModel{
				"master": &PartitionModelState{Priority: 0},
				"slave":  &PartitionModelState{Priority: 1},
			},
			[]string{"slave", "master"},
			[]string{"master", "slave"},
		},
		{
			PartitionModel{
				"master": &PartitionModelState{Priority: 0},
				"slave":  &PartitionModelState{Priority: 1},
			},
			[]string{"a", "b"},
			[]string{"a", "b"},
		},
		{
			PartitionModel{
				"master": &PartitionModelState{Priority: 0},
				"slave":  &PartitionModelState{Priority: 1},
			},
			[]string{"a", "master"},
			[]string{"a", "master"},
		},
		{
			PartitionModel{
				"master": &PartitionModelState{Priority: 0},
				"slave":  &PartitionModelState{Priority: 1},
			},
			[]string{"master", "a"},
			[]string{"a", "master"},
		},
	}
	for i, c := range tests {
		sort.Sort(&stateNameSorter{m: c.m, s: c.s})
		if !reflect.DeepEqual(c.s, c.exp) {
			t.Errorf("i: %d, m: %#v, s: %#v, exp: %#v",
				i, c.m, c.s, c.exp)
		}
	}
}

func TestCountStateNodes(t *testing.T) {
	tests := []struct {
		m   PartitionMap
		w   map[string]int
		exp map[string]map[string]int
	}{
		{
			PartitionMap{
				"0": &Partition{NodesByState: map[string][]string{
					"master": []string{"a"},
					"slave":  []string{"b", "c"},
				}},
				"1": &Partition{NodesByState: map[string][]string{
					"master": []string{"b"},
					"slave":  []string{"c"},
				}},
			},
			nil,
			map[string]map[string]int{
				"master": map[string]int{
					"a": 1,
					"b": 1,
				},
				"slave": map[string]int{
					"b": 1,
					"c": 2,
				},
			},
		},
		{
			PartitionMap{
				"0": &Partition{NodesByState: map[string][]string{
					"slave": []string{"b", "c"},
				}},
				"1": &Partition{NodesByState: map[string][]string{
					"master": []string{"b"},
					"slave":  []string{"c"},
				}},
			},
			nil,
			map[string]map[string]int{
				"master": map[string]int{
					"b": 1,
				},
				"slave": map[string]int{
					"b": 1,
					"c": 2,
				},
			},
		},
	}
	for i, c := range tests {
		r := countStateNodes(c.m, c.w)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, m: %#v, w: %#v, exp: %#v",
				i, c.m, c.w, c.exp)
		}
	}
}

func TestPartitionMapToArrayCopy(t *testing.T) {
	tests := []struct {
		m   PartitionMap
		exp []*Partition
	}{
		{
			PartitionMap{
				"0": &Partition{NodesByState: map[string][]string{
					"master": []string{"a"},
					"slave":  []string{"b", "c"},
				}},
				"1": &Partition{NodesByState: map[string][]string{
					"master": []string{"b"},
					"slave":  []string{"c"},
				}},
			},
			[]*Partition{
				&Partition{NodesByState: map[string][]string{
					"master": []string{"a"},
					"slave":  []string{"b", "c"},
				}},
				&Partition{NodesByState: map[string][]string{
					"master": []string{"b"},
					"slave":  []string{"c"},
				}},
			},
		},
	}
	for i, c := range tests {
		r := c.m.toArrayCopy()
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, m: %#v, r: %#v, exp: %#v",
				i, c.m, r, c.exp)
		}
	}
}
