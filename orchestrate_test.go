package blance

import (
	"testing"
)

func TestOrchestrateMoves(t *testing.T) {
	var partitionModel PartitionModel

	options := OrchestratorOptions{
		MaxConcurrentPartitionBuildsPerCluster: 1,
		MaxConcurrentPartitionBuildsPerNode:    1,
	}

	var nodesAll []string
	var begMap PartitionMap
	var endMap PartitionMap
	var assignPartition AssignPartitionFunc
	var unassignPartition UnassignPartitionFunc
	var partitionState PartitionStateFunc

	o, err := OrchestrateMoves("label",
		partitionModel,
		options,
		nodesAll,
		begMap,
		endMap,
		assignPartition,
		unassignPartition,
		partitionState)
	if err != nil || o == nil {
		t.Errorf("expected o and no err")
	}

	o.Stop()

	_, ok := <-o.ProgressCh()
	if ok {
		t.Errorf("expected progress to be closed")
	}
}
