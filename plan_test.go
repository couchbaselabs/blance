package blance

import (
	"encoding/json"
	"fmt"
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
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b", "c"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"c"},
					},
				},
			},
			[]*Partition{
				&Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b", "c"},
					},
				},
				&Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"c"},
					},
				},
			},
		},
	}
	for _, c := range tests {
		r := c.m.toArrayCopy()
		testSubset := func(a, b []*Partition) {
			if len(a) != len(b) {
				t.Errorf("expected same lengths")
			}
			for _, ap := range a {
				found := false
				for _, bp := range b {
					if reflect.DeepEqual(ap, bp) {
						found = true
					}
				}
				if !found {
					t.Errorf("couldn't find a entry in b")
				}
			}
		}
		testSubset(r, c.exp)
		testSubset(c.exp, r)
	}
}

func TestFindAncestor(t *testing.T) {
	tests := []struct {
		level      int
		mapParents map[string]string
		exp        string
	}{
		{0, map[string]string{}, "a"},
		{1, map[string]string{}, ""},
		{2, map[string]string{}, ""},
		{0, map[string]string{"a": "r"}, "a"},
		{1, map[string]string{"a": "r"}, "r"},
		{2, map[string]string{"a": "r"}, ""},
		{3, map[string]string{"a": "r"}, ""},
		{0, map[string]string{"a": "r", "r": "g"}, "a"},
		{1, map[string]string{"a": "r", "r": "g"}, "r"},
		{2, map[string]string{"a": "r", "r": "g"}, "g"},
		{3, map[string]string{"a": "r", "r": "g"}, ""},
	}
	for i, c := range tests {
		r := findAncestor("a", c.mapParents, c.level)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, level: %d, mapParents: %#v, RESULT: %#v, EXPECTED: %#v",
				i, c.level, c.mapParents, r, c.exp)
		}
	}
}

func TestFindLeaves(t *testing.T) {
	tests := []struct {
		mapChildren map[string][]string
		exp         []string
	}{
		{map[string][]string{}, []string{"a"}},
		{map[string][]string{"x": []string{"xx"}}, []string{"a"}},
		{map[string][]string{"a": []string{}}, []string{"a"}},
		{map[string][]string{"a": []string{"b"}}, []string{"b"}},
		{map[string][]string{"a": []string{"b", "c"}}, []string{"b", "c"}},
	}
	for i, c := range tests {
		r := findLeaves("a", c.mapChildren)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, mapChildren: %#v, RESULT: %#v, EXPECTED: %#v",
				i, c.mapChildren, r, c.exp)
		}
	}
}

func TestMapParentsToMapChildren(t *testing.T) {
	tests := []struct {
		in  map[string]string
		exp map[string][]string
	}{
		{map[string]string{},
			map[string][]string{}},
		{map[string]string{"a": "r"},
			map[string][]string{"r": []string{"a"}}},
		{map[string]string{"a": "r", "b": "r2"},
			map[string][]string{
				"r":  []string{"a"},
				"r2": []string{"b"},
			}},
		{map[string]string{"a": "r", "a1": "a"},
			map[string][]string{
				"r": []string{"a"},
				"a": []string{"a1"},
			}},
	}
	for i, c := range tests {
		r := mapParentsToMapChildren(c.in)
		if !reflect.DeepEqual(r, c.exp) {
			t.Errorf("i: %d, in: %#v, RESULT: %#v, EXPECTED: %#v",
				i, c.in, r, c.exp)
		}
	}
}

func TestPlanNextMap(t *testing.T) {
	tests := []struct {
		About                 string
		PrevMap               PartitionMap
		Nodes                 []string
		NodesToRemove         []string
		NodesToAdd            []string
		Model                 PartitionModel
		ModelStateConstraints map[string]int
		PartitionWeights      map[string]int
		StateStickiness       map[string]int
		NodeWeights           map[string]int
		NodeHierarchy         map[string]string
		HierarchyRules        HierarchyRules
		exp                   PartitionMap
		expNumWarnings        int
	}{
		{
			About: "single node, simple assignment of master",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "single node, not enough to assign slaves",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{},
					},
				},
			},
			expNumWarnings: 2,
		},
		{
			About:         "no partitions case",
			PrevMap:       PartitionMap{},
			Nodes:         []string{"a"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp:                   PartitionMap{},
			expNumWarnings:        0,
		},
		{
			About: "no model states case",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a"},
			Model:         PartitionModel{},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, enough for clean master & slave",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, remove 1",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{"b"},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{},
					},
				},
			},
			expNumWarnings: 2,
		},
		{
			About: "2 nodes, remove 2",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{"b", "a"},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{},
						"slave":  []string{},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{},
						"slave":  []string{},
					},
				},
			},
			expNumWarnings: 4,
		},
		{
			About: "2 nodes, remove 3",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{"c", "b", "a"},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{},
						"slave":  []string{},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{},
						"slave":  []string{},
					},
				},
			},
			expNumWarnings: 4,
		},
		{
			About: "2 nodes, nothing to add or remove",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, swap node a",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{"a"},
			NodesToAdd:    []string{"c"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"c"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"c"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, swap node b",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{"b"},
			NodesToAdd:    []string{"c"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"c"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"c"},
						"slave":  []string{"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "2 nodes, swap nodes a & b for c & d",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			Nodes:         []string{"a", "b", "c", "d"},
			NodesToRemove: []string{"a", "b"},
			NodesToAdd:    []string{"c", "d"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"c"},
						"slave":  []string{"d"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"d"},
						"slave":  []string{"c"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "add 2 nodes, 2 masters, 1 slave",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 2,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a", "b"},
						"slave":  []string{},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"a", "b"},
						"slave":  []string{},
					},
				},
			},
			expNumWarnings: 2,
		},
		{
			About: "add 3 nodes, 2 masters, 1 slave",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b", "c"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b", "c"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 2,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 1,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights:           nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a", "b"},
						"slave":  []string{"c"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"c", "a"},
						"slave":  []string{"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "model state constraint override",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 0,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: map[string]int{
				"master": 1,
				"slave":  1,
			},
			PartitionWeights: nil,
			StateStickiness:  nil,
			NodeWeights:      nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"slave":  []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
						"slave":  []string{"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "partition weight of 3 for partition 0",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights: map[string]int{
				"0": 3,
			},
			StateStickiness: nil,
			NodeWeights:     nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "partition weight of 3 for partition 0, with 4 partitions",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights: map[string]int{
				"0": 3,
			},
			StateStickiness: nil,
			NodeWeights:     nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "partition weight of 3 for partition 1, with 5 partitions",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
				"5": &Partition{
					Name:         "5",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights: map[string]int{
				"1": 3,
			},
			StateStickiness: nil,
			NodeWeights:     nil,
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"5": &Partition{
					Name: "5",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "node weight of 3 for node a",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
				"5": &Partition{
					Name:         "5",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights: map[string]int{
				"a": 3,
			},
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"5": &Partition{
					Name: "5",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
		{
			About: "node weight of 3 for node b",
			PrevMap: PartitionMap{
				"0": &Partition{
					Name:         "0",
					NodesByState: map[string][]string{},
				},
				"1": &Partition{
					Name:         "1",
					NodesByState: map[string][]string{},
				},
				"2": &Partition{
					Name:         "2",
					NodesByState: map[string][]string{},
				},
				"3": &Partition{
					Name:         "3",
					NodesByState: map[string][]string{},
				},
				"4": &Partition{
					Name:         "4",
					NodesByState: map[string][]string{},
				},
				"5": &Partition{
					Name:         "5",
					NodesByState: map[string][]string{},
				},
			},
			Nodes:         []string{"a", "b"},
			NodesToRemove: []string{},
			NodesToAdd:    []string{"a", "b"},
			Model: PartitionModel{
				"master": &PartitionModelState{
					Priority: 0, Constraints: 1,
				},
				"slave": &PartitionModelState{
					Priority: 1, Constraints: 0,
				},
			},
			ModelStateConstraints: nil,
			PartitionWeights:      nil,
			StateStickiness:       nil,
			NodeWeights: map[string]int{
				"b": 3,
			},
			exp: PartitionMap{
				"0": &Partition{
					Name: "0",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"1": &Partition{
					Name: "1",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"2": &Partition{
					Name: "2",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"3": &Partition{
					Name: "3",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
				"4": &Partition{
					Name: "4",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
				"5": &Partition{
					Name: "5",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
			},
			expNumWarnings: 0,
		},
	}
	for i, c := range tests {
		r, rWarnings := PlanNextMap(
			c.PrevMap,
			c.Nodes,
			c.NodesToRemove,
			c.NodesToAdd,
			c.Model,
			c.ModelStateConstraints,
			c.PartitionWeights,
			c.StateStickiness,
			c.NodeWeights,
			c.NodeHierarchy,
			c.HierarchyRules)
		if !reflect.DeepEqual(r, c.exp) {
			jc, _ := json.Marshal(c)
			jr, _ := json.Marshal(r)
			jexp, _ := json.Marshal(c.exp)
			t.Errorf("i: %d, planNextMap, c: %s, [RESULT] r: %s, [EXPECTED] exp: %s",
				i, jc, jr, jexp)
		}
		if c.expNumWarnings != len(rWarnings) {
			t.Errorf("i: %d, planNextMap.warnings, c: %#v, rWarnings: %d, expNumWarnings: %d",
				i, c, rWarnings, c.expNumWarnings)
		}
	}
}

type VisTestCase struct {
	Ignore                bool
	About                 string
	FromTo                [][]string
	Nodes                 []string
	NodesToRemove         []string
	NodesToAdd            []string
	Model                 PartitionModel
	ModelStateConstraints map[string]int
	PartitionWeights      map[string]int
	StateStickiness       map[string]int
	NodeWeights           map[string]int
	NodeHierarchy         map[string]string
	HierarchyRules        HierarchyRules
	expNumWarnings        int
}

func testVisTestCases(t *testing.T, tests []VisTestCase) {
	nodeNames := map[int]string{} // Maps 0 to "a", 1 to "b", etc.
	for i := 0; i < 26; i++ {
		nodeNames[i] = fmt.Sprintf("%c", i+97) // Start at ASCII 'a'.
	}
	stateNames := map[string]string{
		"m": "master",
		"s": "slave",
	}
	for i, c := range tests {
		if c.Ignore {
			continue
		}
		prevMap := PartitionMap{}
		expMap := PartitionMap{}
		for i, partitionFromTo := range c.FromTo {
			partitionName := fmt.Sprintf("%d", i)
			from := partitionFromTo[0]
			to := partitionFromTo[1]

			partition := &Partition{
				Name:         partitionName,
				NodesByState: map[string][]string{},
			}
			prevMap[partitionName] = partition
			for j := 0; j < len(from); j++ {
				stateName := stateNames[from[j:j+1]]
				if stateName != "" {
					partition.NodesByState[stateName] =
						append(partition.NodesByState[stateName], nodeNames[j])
				}
			}

			partition = &Partition{
				Name:         partitionName,
				NodesByState: map[string][]string{},
			}
			expMap[partitionName] = partition
			for j := 0; j < len(to); j++ {
				stateName := stateNames[to[j:j+1]]
				if stateName != "" {
					partition.NodesByState[stateName] =
						append(partition.NodesByState[stateName], nodeNames[j])
				}
			}
		}
		r, rWarnings := PlanNextMap(
			prevMap,
			c.Nodes,
			c.NodesToRemove,
			c.NodesToAdd,
			c.Model,
			c.ModelStateConstraints,
			c.PartitionWeights,
			c.StateStickiness,
			c.NodeWeights,
			c.NodeHierarchy,
			c.HierarchyRules)
		if !reflect.DeepEqual(r, expMap) {
			jc, _ := json.Marshal(c)
			jp, _ := json.Marshal(prevMap)
			jr, _ := json.Marshal(r)
			jexp, _ := json.Marshal(expMap)
			t.Errorf("i: %d, planNextMapVis, c: %s,"+
				"\nINPUT jp: %s,\nRESULT r: %s,\nEXPECTED: %s",
				i, jc, jp, jr, jexp)
		}
		if c.expNumWarnings != len(rWarnings) {
			t.Errorf("i: %d, planNextMapVis.warnings, c: %#v,"+
				" rWarnings: %d, expNumWarnings: %d",
				i, c, rWarnings, c.expNumWarnings)
		}
	}
}

func TestPlanNextMapVis(t *testing.T) {
	partitionModel1Master0Slave := PartitionModel{
		"master": &PartitionModelState{
			Priority: 0, Constraints: 1,
		},
		"slave": &PartitionModelState{
			Priority: 1, Constraints: 0,
		},
	}
	partitionModel1Master1Slave := PartitionModel{
		"master": &PartitionModelState{
			Priority: 0, Constraints: 1,
		},
		"slave": &PartitionModelState{
			Priority: 1, Constraints: 1,
		},
	}
	tests := []VisTestCase{
		{
			About: "single node, simple assignment of master",
			FromTo: [][]string{
				[]string{"", "m"},
				[]string{"", "m"},
			},
			Nodes:          []string{"a"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a"},
			Model:          partitionModel1Master0Slave,
			expNumWarnings: 0,
		},
		{
			About: "added nodes a & b",
			FromTo: [][]string{
				[]string{"", "ms"},
				[]string{"", "sm"},
			},
			Nodes:          []string{"a", "b"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			About: "single node to 2 nodes",
			FromTo: [][]string{
				[]string{"m", "sm"},
				[]string{"m", "ms"},
			},
			Nodes:          []string{"a", "b"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			About: "single node to 3 nodes",
			FromTo: [][]string{
				[]string{"m", "sm "},
				[]string{"m", "m s"},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			About: "2 unbalanced nodes to balanced'ness",
			FromTo: [][]string{
				[]string{"ms", "sm"},
				[]string{"ms", "ms"},
			},
			Nodes:          []string{"a", "b"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			About: "2 unbalanced nodes to 3 balanced nodes",
			FromTo: [][]string{
				[]string{"ms", " sm"},
				[]string{"ms", "m s"},
			},
			Nodes:          []string{"a", "b", "c"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"c"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			About: "4 partitions, 1 to 4 nodes",
			FromTo: [][]string{
				[]string{"m", "sm  "},
				[]string{"m", "  ms"},
				[]string{"m", "  sm"},
				[]string{"m", "ms  "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			// ISSUE: Looks like node b is assigned too many slaves?
			// Re-running the algorithm again, however, seems to
			// stabilize (see next two cases).
			About: "8 partitions, 1 to 4 nodes",
			FromTo: [][]string{
				//             abcd
				[]string{"m", "sm  "},
				[]string{"m", "  ms"},
				[]string{"m", "s  m"},
				[]string{"m", " ms "},
				[]string{"m", " sm "},
				[]string{"m", " s m"},
				[]string{"m", "ms  "},
				[]string{"m", "m s "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			// Take output from previous case and use as input to this
			// case to see that it reached even more balanced'ness.
			About: "8 partitions, 4 nodes don't change, 1 slave moved",
			FromTo: [][]string{
				//        abcd    abcd
				[]string{"sm  ", "sm  "},
				[]string{"  ms", "  ms"},
				[]string{"s  m", "s  m"},
				[]string{" ms ", " ms "},
				[]string{" sm ", "  ms"}, // Slave moved to d for more balanced'ness.
				[]string{" s m", " s m"},
				[]string{"ms  ", "ms  "},
				[]string{"m s ", "m s "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			// Take output from previous case and use as input to this
			// case, and see that it stabilized...
			About: "8 partitions, 4 nodes don't change, so no changes",
			FromTo: [][]string{
				//        abcd    abcd
				[]string{"sm  ", "sm  "},
				[]string{"  ms", "  ms"},
				[]string{"s  m", "s  m"},
				[]string{" ms ", " ms "},
				[]string{" sm ", "  ms"},
				[]string{" s m", " s m"},
				[]string{"ms  ", "ms  "},
				[]string{"m s ", "m s "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			// TODO: ISSUE: looks like the masters assigned to b moved
			// nicely to node e, but b's slaves didn't go to e
			// cleanly.
			About: "single node swap, from node b to node e",
			FromTo: [][]string{
				//        abcd    abcde
				[]string{" m s", "   sm"},
				[]string{"  ms", "  m s"}, // Non-optimal slave move?
				[]string{"s  m", "s  m "},
				[]string{" ms ", "  s m"},
				[]string{" sm ", "  ms "}, // c instead of e took over b's slave?
				[]string{"s  m", "s  m "},
				[]string{"ms  ", "m   s"},
				[]string{"m s ", "m s  "},
			},
			Nodes:          []string{"a", "b", "c", "d", "e"},
			NodesToRemove:  []string{"b"},
			NodesToAdd:     []string{"e"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			// Masters stayed nicely stable during node removal.
			// TODO: But, perhaps node a has too much load.
			About: "4 nodes to 3 nodes, remove node d",
			FromTo: [][]string{
				//        abcd    abc
				[]string{" m s", "sm "},
				[]string{"  ms", "s m"},
				[]string{"s  m", "m s"},
				[]string{" ms ", " ms"},
				[]string{" sm ", " sm"},
				[]string{"s  m", "sm "},
				[]string{"ms  ", "ms "},
				[]string{"m s ", "m s"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{"d"},
			NodesToAdd:     []string{},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			// TODO: ISSUE: the slaves aren't cleared when we change
			// the constraints from 1 slave down to 0 slaves, so
			// ignore this case for now.
			Ignore: true,
			About:  "change constraints from 1 slave to 0 slaves",
			FromTo: [][]string{
				//        abcd    abcd
				[]string{" m s", " m  "},
				[]string{"  ms", "  m "},
				[]string{"s  m", "   m"},
				[]string{" ms ", " m  "},
				[]string{" sm ", "  m "},
				[]string{"s  m", "   m"},
				[]string{"ms  ", "m   "},
				[]string{"m s ", "m   "},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{},
			Model:          partitionModel1Master0Slave,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 1 to 8 nodes",
			FromTo: [][]string{
				//             abcdefgh
				[]string{"m", "s      m"},
				[]string{"m", "  s   m "},
				[]string{"m", "   s m  "},
				[]string{"m", "    ms  "},
				[]string{"m", " m  s   "},
				[]string{"m", "   m  s "},
				[]string{"m", "  m    s"},
				[]string{"m", "ms      "},
			},
			Nodes:          []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d", "e", "f", "g", "h"},
			Model:          partitionModel1Master1Slave,
			expNumWarnings: 0,
		},
		{
			About: "8 partitions, 1 to 8 nodes, 0 slaves",
			FromTo: [][]string{
				//             abcdefgh
				[]string{"m", "       m"},
				[]string{"m", "      m "},
				[]string{"m", "     m  "},
				[]string{"m", "    m   "},
				[]string{"m", " m      "},
				[]string{"m", "   m    "},
				[]string{"m", "  m     "},
				[]string{"m", "m       "},
			},
			Nodes:          []string{"a", "b", "c", "d", "e", "f", "g", "h"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"b", "c", "d", "e", "f", "g", "h"},
			Model:          partitionModel1Master0Slave,
			expNumWarnings: 0,
		},
	}
	testVisTestCases(t, tests)
}

func TestPlanNextMapHierarchy(t *testing.T) {
	partitionModel1Master1Slave := PartitionModel{
		"master": &PartitionModelState{
			Priority: 0, Constraints: 1,
		},
		"slave": &PartitionModelState{
			Priority: 1, Constraints: 1,
		},
	}
	nodeHierarchy2Rack := map[string]string{
		"a": "r0",
		"b": "r0",
		"c": "r1",
		"d": "r1",

		// Racks r0 and r1 in the same zone z0.
		"r0": "z0",
		"r1": "z0",
	}
	hierarchyRulesWantSameRack := HierarchyRules{
		"slave": []*HierarchyRule{
			&HierarchyRule{
				IncludeLevel: 1,
				ExcludeLevel: 0,
			},
		},
	}
	hierarchyRulesWantOtherRack := HierarchyRules{
		"slave": []*HierarchyRule{
			&HierarchyRule{
				IncludeLevel: 2,
				ExcludeLevel: 1,
			},
		},
	}
	tests := []VisTestCase{
		{
			About: "2 racks, but nil hierarchy rules",
			FromTo: [][]string{
				//            abcd
				[]string{"", "ms  "},
				[]string{"", "sm  "},
				[]string{"", "  ms"},
				[]string{"", "  sm"},
				[]string{"", "m s "},
				[]string{"", " m s"},
				[]string{"", "s m "},
				[]string{"", " s m"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Master1Slave,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: nil,
			expNumWarnings: 0,
		},
		{
			About: "2 racks, favor same rack for slave",
			FromTo: [][]string{
				//            abcd
				[]string{"", "ms  "},
				[]string{"", "sm  "},
				[]string{"", "  ms"},
				[]string{"", "  sm"},
				[]string{"", "ms  "},
				[]string{"", "sm  "},
				[]string{"", "  ms"},
				[]string{"", "  sm"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Master1Slave,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: hierarchyRulesWantSameRack,
			expNumWarnings: 0,
		},
		{
			About: "2 racks, favor other rack for slave",
			FromTo: [][]string{
				//            abcd
				[]string{"", "m  s"},
				[]string{"", " ms "},
				[]string{"", "s m "},
				[]string{"", " s m"},
				[]string{"", "m s "},
				[]string{"", " m s"},
				[]string{"", " sm "},
				[]string{"", "s  m"},
			},
			Nodes:          []string{"a", "b", "c", "d"},
			NodesToRemove:  []string{},
			NodesToAdd:     []string{"a", "b", "c", "d"},
			Model:          partitionModel1Master1Slave,
			NodeHierarchy:  nodeHierarchy2Rack,
			HierarchyRules: hierarchyRulesWantOtherRack,
			expNumWarnings: 0,
		},
	}
	testVisTestCases(t, tests)
}
