package blance

import (
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
		assignPartition AssignPartitionFunc
		partitionState  PartitionStateFunc
		expErr          error
	}{
		{
			label:           "label",
			partitionModel:  mrPartitionModel,
			options:         options_1_1,
			nodesAll:        []string(nil),
			begMap:          PartitionMap{},
			endMap:          PartitionMap{},
			assignPartition: nil,
			partitionState:  nil,
		},
	}

	for testi, test := range tests {
		o, err := OrchestrateMoves(test.label,
			test.partitionModel,
			test.options,
			test.nodesAll,
			test.begMap,
			test.endMap,
			test.assignPartition,
			test.partitionState)
		if o == nil {
			t.Errorf("testi: %d, label: %s,"+
				" expected o",
				testi, test.label)
		}
		if err != test.expErr {
			t.Errorf("testi: %d, label: %s,"+
				"expected err: %v, got: %v",
				testi, test.label,
				test.expErr, err)
		}

		o.Stop()

		_, ok := <-o.ProgressCh()
		if ok {
			t.Errorf("testi: %d, label: %s,"+
				"expected progress to be closed",
				testi, test.label)
		}
	}
}
