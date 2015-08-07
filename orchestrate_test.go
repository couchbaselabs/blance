package blance

import (
	"sync"
	"testing"
)

type assignPartitionRec struct {
	partition string
	node      string
	state     string
}

func TestOrchestrateMoves(t *testing.T) {
	mrPartitionModel := PartitionModel{
		"master": &PartitionModelState{
			Priority: 0,
		},
		"replica": &PartitionModelState{
			Constraints: 1,
		},
	}

	options_1_1 := OrchestratorOptions{
		MaxConcurrentPartitionBuildsPerCluster: 1,
		MaxConcurrentPartitionBuildsPerNode:    1,
	}

	tests := []struct {
		label          string
		partitionModel PartitionModel
		options        OrchestratorOptions
		nodesAll       []string
		begMap         PartitionMap
		endMap         PartitionMap
		expectErr      error

		expectAssignPartitions []assignPartitionRec
	}{
		{
			label:          "do nothing",
			partitionModel: mrPartitionModel,
			options:        options_1_1,
			nodesAll:       []string(nil),
			begMap:         PartitionMap{},
			endMap:         PartitionMap{},
			expectErr:      nil,
		},
		{
			label:          "1 node, no assignments or changes",
			partitionModel: mrPartitionModel,
			options:        options_1_1,
			nodesAll:       []string{"a"},
			begMap:         PartitionMap{},
			endMap:         PartitionMap{},
			expectErr:      nil,
		},
		{
			label:          "no nodes, but some partitions",
			partitionModel: mrPartitionModel,
			options:        options_1_1,
			nodesAll:       []string(nil),
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
				"01": &Partition{
					Name:         "01",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
				"01": &Partition{
					Name:         "01",
					NodesByState: map[string][]string{},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options_1_1,
			nodesAll:       []string{"a"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
			},
			expectAssignPartitions: []assignPartitionRec{
				assignPartitionRec{
					partition: "00", node: "a", state: "master",
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options_1_1,
			nodesAll:       []string{"a", "b"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"replica": []string{"b"},
					},
				},
			},
			expectAssignPartitions: []assignPartitionRec{
				assignPartitionRec{
					partition: "00", node: "a", state: "master",
				},
				assignPartitionRec{
					partition: "00", node: "b", state: "replica",
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b & c, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options_1_1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master": []string{"a"},
						"replica": []string{"b"},
					},
				},
			},
			expectAssignPartitions: []assignPartitionRec{
				assignPartitionRec{
					partition: "00", node: "a", state: "master",
				},
				assignPartitionRec{
					partition: "00", node: "b", state: "replica",
				},
			},
			expectErr: nil,
		},
	}

	for testi, test := range tests {
		var m sync.Mutex

		var assignPartitionRecs []assignPartitionRec

		assignPartitionFunc := func(partition, node, state string) error {
			m.Lock()
			assignPartitionRecs = append(assignPartitionRecs,
				assignPartitionRec{partition, node, state})
			m.Unlock()

			return nil
		}

		partitionStateFunc := func(partition string, node string) (
			state string, pct float32, err error) {
			return "", 0, nil
		}

		o, err := OrchestrateMoves(test.label,
			test.partitionModel,
			test.options,
			test.nodesAll,
			test.begMap,
			test.endMap,
			assignPartitionFunc,
			partitionStateFunc)
		if o == nil {
			t.Errorf("testi: %d, label: %s,"+
				" expected o",
				testi, test.label)
		}
		if err != test.expectErr {
			t.Errorf("testi: %d, label: %s,"+
				" expectErr: %v, got: %v",
				testi, test.label,
				test.expectErr, err)
		}

		for range o.ProgressCh() {
			// TODO: Should check for expected progress.
		}

		if len(assignPartitionRecs) != len(test.expectAssignPartitions) {
			t.Errorf("testi: %d, label: %s,"+
				" len(assignPartitionRecs == %d)"+
				" != len(test.expectAssignPartitions == %d),"+
				" assignPartitionRecs: %#v,"+
				" test.expectAssignPartitions: %#v",
				testi, test.label,
				len(assignPartitionRecs),
				len(test.expectAssignPartitions),
				assignPartitionRecs,
				test.expectAssignPartitions)
		}

		for eapi, eap := range test.expectAssignPartitions {
			apr := assignPartitionRecs[eapi]
			if eap.partition != apr.partition ||
				eap.node != apr.node ||
				eap.state != apr.state {
				t.Errorf("testi: %d, label: %s,"+
					" mismatched assignment,"+
					" eapi: %d, eap: %#v, apr: %#v",
					testi, test.label,
					eapi, eap, apr)
			}
		}
	}
}
