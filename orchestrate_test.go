package blance

import (
	"sync"
	"testing"
)

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

	tests := []struct{
		label           string
		partitionModel  PartitionModel
		options         OrchestratorOptions
		nodesAll        []string
		begMap          PartitionMap
		endMap          PartitionMap
		expectErr       error
	}{
		{
			label:           "do nothing",
			partitionModel:  mrPartitionModel,
			options:         options_1_1,
			nodesAll:        []string(nil),
			begMap:          PartitionMap{},
			endMap:          PartitionMap{},
			expectErr:       nil,
		},
		{
			label:           "1 node, no assignments or changes",
			partitionModel:  mrPartitionModel,
			options:         options_1_1,
			nodesAll:        []string{"a"},
			begMap:          PartitionMap{},
			endMap:          PartitionMap{},
			expectErr:       nil,
		},
	}

	type assignPartitionRec struct {
		partition string
		node      string
		state     string
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

		go func() {
			for range o.ProgressCh() {
			}
		}()

		o.Stop()
	}
}
