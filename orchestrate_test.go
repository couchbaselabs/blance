package blance

import (
	"fmt"
	"sync"
	"testing"
)

type assignPartitionRec struct {
	partition string
	node      string
	state     string
	op        string
}

var mrPartitionModel = PartitionModel{
	"master": &PartitionModelState{
		Priority: 0,
	},
	"replica": &PartitionModelState{
		Constraints: 1,
	},
}

var options1 = OrchestratorOptions{
	MaxConcurrentPartitionMovesPerNode: 1,
}

func TestOrchestrateBadMoves(t *testing.T) {
	o, err := OrchestrateMoves(
		mrPartitionModel,
		options1,
		nil,
		PartitionMap{
			"00": &Partition{
				Name:         "00",
				NodesByState: map[string][]string{},
			},
			"01": &Partition{
				Name:         "01",
				NodesByState: map[string][]string{},
			},
		},
		PartitionMap{
			"01": &Partition{
				Name:         "01",
				NodesByState: map[string][]string{},
			},
		},
		nil,
		nil,
		nil,
	)
	if err == nil || o != nil {
		t.Errorf("expected err on mismatched beg/end maps")
	}
}

func TestOrchestrateErrAssignPartitionFunc(t *testing.T) {
	theErr := fmt.Errorf("theErr")

	errAssignPartitionFunc := func(stopCh chan struct{},
		partition, node, state, op string) error {
		return theErr
	}

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master": []string{"a"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master": []string{"b"},
				},
			},
		},
		errAssignPartitionFunc,
		nil,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress
	}

	o.Stop()

	if gotProgress <= 0 {
		t.Errorf("expected progress")
	}

	if len(lastProgress.Errors) <= 0 {
		t.Errorf("expected errs")
	}
}

func testMkFuncs() (
	map[string]map[string]string,
	map[string][]assignPartitionRec,
	AssignPartitionFunc,
	PartitionStateFunc,
) {
	var m sync.Mutex

	// Map of partition -> node -> state.
	currStates := map[string]map[string]string{}

	assignPartitionRecs := map[string][]assignPartitionRec{}

	assignPartitionFunc := func(stopCh chan struct{},
		partition, node, state, op string) error {
		m.Lock()

		assignPartitionRecs[partition] =
			append(assignPartitionRecs[partition],
				assignPartitionRec{partition, node, state, op})

		nodes := currStates[partition]
		if nodes == nil {
			nodes = map[string]string{}
			currStates[partition] = nodes
		}

		nodes[node] = state

		m.Unlock()

		return nil
	}

	partitionStateFunc := func(stopCh chan struct{},
		partition string, node string) (
		state string, pct float32, err error) {
		m.Lock()
		currState := currStates[partition][node]
		m.Unlock()
		return currState, 1.0, nil
	}

	return currStates, assignPartitionRecs,
		assignPartitionFunc, partitionStateFunc
}

func TestOrchestrateEarlyPauseResume(t *testing.T) {
	testOrchestratePauseResume(t, 1)
}

func TestOrchestrateMidPauseResume(t *testing.T) {
	testOrchestratePauseResume(t, 2)
}

func testOrchestratePauseResume(t *testing.T, numProgress int) {
	_, _, assignPartitionFunc, partitionStateFunc := testMkFuncs()

	pauseCh := make(chan struct{})

	slowPartitionStateFunc := func(stopCh chan struct{},
		partition string, node string) (
		state string, pct float32, err error) {
		<-pauseCh
		return partitionStateFunc(stopCh, partition, node)
	}

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master":  []string{"a"},
					"replica": []string{"b"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"master":  []string{"a"},
					"replica": []string{"b"},
				},
			},
			"02": &Partition{
				Name: "02",
				NodesByState: map[string][]string{
					"master":  []string{"a"},
					"replica": []string{"b"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master":  []string{"b"},
					"replica": []string{"a"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"master":  []string{"b"},
					"replica": []string{"a"},
				},
			},
			"02": &Partition{
				Name: "02",
				NodesByState: map[string][]string{
					"master":  []string{"b"},
					"replica": []string{"a"},
				},
			},
		},
		assignPartitionFunc,
		slowPartitionStateFunc,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	for i := 0; i < numProgress; i++ {
		<-o.ProgressCh()
	}

	o.PauseNewAssignments()
	o.PauseNewAssignments()
	o.PauseNewAssignments()

	o.ResumeNewAssignments()
	o.ResumeNewAssignments()

	close(pauseCh)

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress

		o.ResumeNewAssignments()
	}

	o.Stop()

	if gotProgress <= 0 {
		t.Errorf("expected progress")
	}

	if len(lastProgress.Errors) > 0 {
		t.Errorf("expected no errs")
	}

	if lastProgress.TotPauseNewAssignments != 1 ||
		lastProgress.TotResumeNewAssignments != 1 {
		t.Errorf("numProgress: %d, expected pause/resume of 1, got: %#v",
			numProgress, lastProgress)
	}
}

func TestOrchestrateErrPartitionState(t *testing.T) {
	_, _, assignPartitionFunc, _ := testMkFuncs()

	theErr := fmt.Errorf("theErr")

	errPartitionStateFunc := func(stopCh chan struct{},
		partition string, node string) (
		state string, pct float32, err error) {
		return "", 0.0, theErr
	}

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master":  []string{"a"},
					"replica": []string{"b"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"master":  []string{"a"},
					"replica": []string{"b"},
				},
			},
			"02": &Partition{
				Name: "02",
				NodesByState: map[string][]string{
					"master":  []string{"a"},
					"replica": []string{"b"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master":  []string{"b"},
					"replica": []string{"a"},
				},
			},
			"01": &Partition{
				Name: "01",
				NodesByState: map[string][]string{
					"master":  []string{"b"},
					"replica": []string{"a"},
				},
			},
			"02": &Partition{
				Name: "02",
				NodesByState: map[string][]string{
					"master":  []string{"b"},
					"replica": []string{"a"},
				},
			},
		},
		assignPartitionFunc,
		errPartitionStateFunc,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress
	}

	o.Stop()

	if gotProgress <= 0 {
		t.Errorf("expected progress")
	}

	if len(lastProgress.Errors) <= 0 {
		t.Errorf("expected errs")
	}
}

func TestOrchestrateEarlyStop(t *testing.T) {
	_, _, assignPartitionFunc, partitionStateFunc := testMkFuncs()

	o, err := OrchestrateMoves(
		mrPartitionModel,
		OrchestratorOptions{},
		[]string{"a", "b"},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master": []string{"a"},
				},
			},
		},
		PartitionMap{
			"00": &Partition{
				Name: "00",
				NodesByState: map[string][]string{
					"master": []string{"b"},
				},
			},
		},
		assignPartitionFunc,
		partitionStateFunc,
		LowestWeightPartitionMoveForNode,
	)
	if err != nil || o == nil {
		t.Errorf("expected nil err")
	}

	<-o.ProgressCh()

	o.Stop()
	o.Stop()
	o.Stop()

	gotProgress := 0
	var lastProgress OrchestratorProgress

	for progress := range o.ProgressCh() {
		gotProgress++
		lastProgress = progress
	}

	if gotProgress <= 0 {
		t.Errorf("expected some progress")
	}

	if len(lastProgress.Errors) > 0 {
		t.Errorf("expected no errs")
	}

	if lastProgress.TotStop != 1 {
		t.Errorf("expected stop of 1")
	}
}

func TestOrchestrateMoves(t *testing.T) {
	tests := []struct {
		skip           bool
		label          string
		partitionModel PartitionModel
		options        OrchestratorOptions
		nodesAll       []string
		begMap         PartitionMap
		endMap         PartitionMap
		expectErr      error

		// Keyed by partition.
		expectAssignPartitions map[string][]assignPartitionRec
	}{
		{
			label:          "do nothing",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string(nil),
			begMap:         PartitionMap{},
			endMap:         PartitionMap{},
			expectErr:      nil,
		},
		{
			label:          "1 node, no assignments or changes",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a"},
			begMap:         PartitionMap{},
			endMap:         PartitionMap{},
			expectErr:      nil,
		},
		{
			label:          "no nodes, but some partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
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
			options:        options1,
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
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "a", state: "master",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
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
						"master":  []string{"a"},
						"replica": []string{"b"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "a", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "b", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b & c, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
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
						"master":  []string{"a"},
						"replica": []string{"b"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "a", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "b", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "del node a, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name:         "00",
					NodesByState: map[string][]string{},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "swap a to b, 1 partition",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master": []string{"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master": []string{"b"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "b", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "swap a to b, 1 partition, c unchanged",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"a"},
						"replica": []string{"c"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"c"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "b", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "1 partition from a|b to c|a",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"a"},
						"replica": []string{"b"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"c"},
						"replica": []string{"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "c", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "a", state: "replica",
					},
					assignPartitionRec{
						partition: "00", node: "b", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "add node a & b, 2 partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b"},
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
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"a"},
						"replica": []string{"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "a", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "b", state: "replica",
					},
				},
				"01": []assignPartitionRec{
					assignPartitionRec{
						partition: "01", node: "b", state: "master",
					},
					assignPartitionRec{
						partition: "01", node: "a", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "swap ab to cd, 2 partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c", "d"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"a"},
						"replica": []string{"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"c"},
						"replica": []string{"d"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"d"},
						"replica": []string{"c"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "c", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "a", state: "",
					},
					assignPartitionRec{
						partition: "00", node: "d", state: "replica",
					},
					assignPartitionRec{
						partition: "00", node: "b", state: "",
					},
				},
				"01": []assignPartitionRec{
					assignPartitionRec{
						partition: "01", node: "d", state: "master",
					},
					assignPartitionRec{
						partition: "01", node: "b", state: "",
					},
					assignPartitionRec{
						partition: "01", node: "c", state: "replica",
					},
					assignPartitionRec{
						partition: "01", node: "a", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			// TODO: This test is intended to get coverage on
			// LowestWeightPartitionMoveForNode() on its inner
			// MoveOpWeight if statement, but seems to be
			// intermittent -- perhaps goroutine race?
			label:          "concurrent moves on b, 2 partitions",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"a"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"a"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"a"},
						"replica": []string{"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"c"},
						"replica": []string{"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "a", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "b", state: "replica",
					},
				},
				"01": []assignPartitionRec{
					assignPartitionRec{
						partition: "01", node: "c", state: "master",
					},
					assignPartitionRec{
						partition: "01", node: "b", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "nodes with not much work",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c", "d", "e"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"a", "d", "e"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"a", "d", "e"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"a"},
						"replica": []string{"b", "d", "e"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"c"},
						"replica": []string{"a", "d", "e"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "a", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "b", state: "replica",
					},
				},
				"01": []assignPartitionRec{
					assignPartitionRec{
						partition: "01", node: "c", state: "master",
					},
					assignPartitionRec{
						partition: "01", node: "b", state: "",
					},
				},
			},
			expectErr: nil,
		},
		{
			label:          "more concurrent moves",
			partitionModel: mrPartitionModel,
			options:        options1,
			nodesAll:       []string{"a", "b", "c", "d", "e", "f", "g"},
			begMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"a"},
						"replica": []string{"b"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"c"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"master":  []string{"c"},
						"replica": []string{"d"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"master":  []string{"d"},
						"replica": []string{"e"},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"master":  []string{"e"},
						"replica": []string{"f"},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"master":  []string{"f"},
						"replica": []string{"g"},
					},
				},
			},
			endMap: PartitionMap{
				"00": &Partition{
					Name: "00",
					NodesByState: map[string][]string{
						"master":  []string{"b"},
						"replica": []string{"c"},
					},
				},
				"01": &Partition{
					Name: "01",
					NodesByState: map[string][]string{
						"master":  []string{"c"},
						"replica": []string{"d"},
					},
				},
				"02": &Partition{
					Name: "02",
					NodesByState: map[string][]string{
						"master":  []string{"d"},
						"replica": []string{"e"},
					},
				},
				"03": &Partition{
					Name: "03",
					NodesByState: map[string][]string{
						"master":  []string{"e"},
						"replica": []string{"f"},
					},
				},
				"04": &Partition{
					Name: "04",
					NodesByState: map[string][]string{
						"master":  []string{"f"},
						"replica": []string{"g"},
					},
				},
				"05": &Partition{
					Name: "05",
					NodesByState: map[string][]string{
						"master":  []string{"g"},
						"replica": []string{"a"},
					},
				},
			},
			expectAssignPartitions: map[string][]assignPartitionRec{
				"00": []assignPartitionRec{
					assignPartitionRec{
						partition: "00", node: "b", state: "master",
					},
					assignPartitionRec{
						partition: "00", node: "a", state: "",
					},
					assignPartitionRec{
						partition: "00", node: "c", state: "replica",
					},
				},
				"01": []assignPartitionRec{
					assignPartitionRec{
						partition: "01", node: "c", state: "master",
					},
					assignPartitionRec{
						partition: "01", node: "b", state: "",
					},
					assignPartitionRec{
						partition: "01", node: "d", state: "replica",
					},
				},
				"02": []assignPartitionRec{
					assignPartitionRec{
						partition: "02", node: "d", state: "master",
					},
					assignPartitionRec{
						partition: "02", node: "c", state: "",
					},
					assignPartitionRec{
						partition: "02", node: "e", state: "replica",
					},
				},
				"03": []assignPartitionRec{
					assignPartitionRec{
						partition: "03", node: "e", state: "master",
					},
					assignPartitionRec{
						partition: "03", node: "d", state: "",
					},
					assignPartitionRec{
						partition: "03", node: "f", state: "replica",
					},
				},
				"04": []assignPartitionRec{
					assignPartitionRec{
						partition: "04", node: "f", state: "master",
					},
					assignPartitionRec{
						partition: "04", node: "e", state: "",
					},
					assignPartitionRec{
						partition: "04", node: "g", state: "replica",
					},
				},
				"05": []assignPartitionRec{
					assignPartitionRec{
						partition: "05", node: "g", state: "master",
					},
					assignPartitionRec{
						partition: "05", node: "f", state: "",
					},
					assignPartitionRec{
						partition: "05", node: "a", state: "replica",
					},
				},
			},
			expectErr: nil,
		},
	}

	for testi, test := range tests {
		if test.skip {
			continue
		}

		_, assignPartitionRecs,
			assignPartitionFunc, partitionStateFunc := testMkFuncs()

		o, err := OrchestrateMoves(
			test.partitionModel,
			test.options,
			test.nodesAll,
			test.begMap,
			test.endMap,
			assignPartitionFunc,
			partitionStateFunc,
			LowestWeightPartitionMoveForNode,
		)
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

		debug := false

		if debug {
			o.m.Lock()
			fmt.Printf("test: %q\n  START progress: %#v\n",
				test.label, o.progress)
			o.m.Unlock()
		}

		for progress := range o.ProgressCh() {
			if debug {
				fmt.Printf("test: %q\n  progress: %#v\n",
					test.label, progress)
			}
		}

		o.Stop()

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

		for partition, eapm := range test.expectAssignPartitions {
			for eapi, eap := range eapm {
				apr := assignPartitionRecs[partition][eapi]
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
}
