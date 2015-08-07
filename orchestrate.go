//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package blance

import (
	"sort"
	"sync"
)

// An Orchestrator instance holds the runtime state during an
// OrchestrateMoves() operation.
type Orchestrator struct {
	label string

	model PartitionModel

	options OrchestratorOptions

	nodesAll []string

	begMap PartitionMap
	endMap PartitionMap

	assignPartition   AssignPartitionFunc
	unassignPartition UnassignPartitionFunc
	partitionState    PartitionStateFunc

	progressCh chan OrchestratorProgress

	tokensSupplyCh  chan int
	tokensReleaseCh chan int

	mapNodeToPartitionMoveCh map[string]chan partitionMove

	m sync.Mutex // Protects the fields that follow.

	stopCh   chan struct{} // Becomes nil when stopped.
	pauseCh  chan struct{} // May be nil; non-nil when paused.
	progress OrchestratorProgress

	mapPartitionToNextMoves map[string]*nextMoves
}

type OrchestratorOptions struct {
	MaxConcurrentPartitionBuildsPerCluster int
	MaxConcurrentPartitionBuildsPerNode    int
}

type OrchestratorProgress struct {
	Errors                      []error
	TotPartitionsAssigned       int
	TotPartitionsAssignedDone   int
	TotPartitionsUnassigned     int
	TotPartitionsUnassignedDone int
	TotRunMover                 int
	TotRunMoverDone             int
	TotRunMoverDoneErr          int
}

// AssignPartitionFunc is a callback invoked by OrchestrateMoves()
// when it wants to asynchronously assign a partition to a node.
type AssignPartitionFunc func(
	partition string,
	node string,
	state string,
	insertAt int,
	fromNode string,
	fromNodeTakeOver bool) error

// UnassignPartitionFunc is a callback invoked by OrchestrateMoves()
// when it wants to asynchronously remove a partition from a node.
type UnassignPartitionFunc func(
	partition string,
	node string,
	state string) error

// UnassignPartitionFunc is a callback invoked by OrchestrateMoves()
// when it wants to synchronously retrieve information about a
// partition on a node.
type PartitionStateFunc func(
	partition string,
	node string) (
	state string,
	position int,
	pct float32,
	err error)

// ------------------------------------------

type partitionMove struct {
	partition string
	state     string
	op        string // See NodeStateOp.Op field.
}

type nextMoves struct {
	partition string
	next      int // Index into moves array that is our next move.
	moves     []NodeStateOp
}

// ------------------------------------------

// OrchestratorMoves asynchronously begins reassigning partitions
// amongst nodes to transition from the begMap to the endMap state,
// invoking the callback functions like assignPartition() and
// unassignPartition() to affect changes.  Additionally, the caller
// must read the progress channel until it's closed by
// OrchestrateMoves to avoid blocking the orchestration.
func OrchestrateMoves(
	label string,
	model PartitionModel,
	options OrchestratorOptions,
	nodesAll []string,
	begMap PartitionMap,
	endMap PartitionMap,
	assignPartition AssignPartitionFunc,
	unassignPartition UnassignPartitionFunc,
	partitionState PartitionStateFunc,
) (*Orchestrator, error) {
	m := options.MaxConcurrentPartitionBuildsPerCluster
	n := options.MaxConcurrentPartitionBuildsPerNode

	// The mapNodeToPartitionMoveCh is keyed by node name.
	mapNodeToPartitionMoveCh := map[string]chan partitionMove{}
	for _, node := range nodesAll {
		mapNodeToPartitionMoveCh[node] = make(chan partitionMove)
	}

	states := sortStateNames(model)

	// The mapPartitionToNextMoves is keyed by partition name.
	mapPartitionToNextMoves := map[string]*nextMoves{}

	for partitionName, begPartition := range begMap {
		endPartition := endMap[partitionName]

		moves := CalcPartitionMoves(states,
			begPartition.NodesByState,
			endPartition.NodesByState)

		mapPartitionToNextMoves[partitionName] = &nextMoves{
			partition: partitionName,
			next:      0,
			moves:     moves,
		}
	}

	o := &Orchestrator{
		label:             label,
		model:             model,
		options:           options,
		nodesAll:          nodesAll,
		begMap:            begMap,
		endMap:            endMap,
		assignPartition:   assignPartition,
		unassignPartition: unassignPartition,
		partitionState:    partitionState,
		progressCh:        make(chan OrchestratorProgress),
		tokensSupplyCh:    make(chan int, m),
		tokensReleaseCh:   make(chan int, m),

		mapNodeToPartitionMoveCh: mapNodeToPartitionMoveCh,

		stopCh:  make(chan struct{}),
		pauseCh: nil,

		mapPartitionToNextMoves: mapPartitionToNextMoves,
	}

	stopCh := o.stopCh

	runMoverDoneCh := make(chan error)

	// Start concurrent movers.
	for _, node := range o.nodesAll {
		for i := 0; i < n; i++ {
			go func() {
				o.m.Lock()
				o.progress.TotRunMover++
				o.m.Unlock()

				runMoverDoneCh <- o.runMover(node, stopCh)
			}()
		}
	}

	// Supply tokens to movers.
	go o.runTokens(m)

	// Feed moves to the movers.
	go o.runPartitionMoveFeeder()

	go func() { // Wait for movers to finish and then cleanup.
		for i := 0; i < len(o.nodesAll)*n; i++ {
			err := <-runMoverDoneCh

			o.m.Lock()
			o.progress.TotRunMoverDone++
			if err != nil {
				o.progress.Errors = append(o.progress.Errors, err)
				o.progress.TotRunMoverDoneErr++
			}
			progress := o.progress
			o.m.Unlock()

			o.progressCh <- progress
		}

		close(o.tokensReleaseCh)

		close(o.progressCh)
	}()

	return o, nil
}

// Stop() asynchronously requests the orchestrator to stop, where the
// caller will eventually see a closed progress channel.
func (o *Orchestrator) Stop() {
	o.m.Lock()
	if o.stopCh != nil {
		close(o.stopCh)
		o.stopCh = nil
	}
	o.m.Unlock()
}

// ProgressCh() returns a channel that is updated occassionally when
// the orchestrator has made some progress on one or more partition
// reassignments, or has reached an error.  The channel is closed by
// the orchestrator when it is finished, either naturally, or due to
// an error, or via a Stop(), and all the orchestrator's resources
// have been released.
func (o *Orchestrator) ProgressCh() chan OrchestratorProgress {
	return o.progressCh
}

// PauseNewAssignments() disallows the orchestrator from starting any
// new assignments of partitions to nodes.  Any inflight partition
// moves will continue to be finished.  The caller can monitor the
// ProgressCh to determine when to pause and/or resume partition
// assignments.  PauseNewAssignments is idempotent.
func (o *Orchestrator) PauseNewAssignments() error {
	o.m.Lock()
	if o.pauseCh == nil {
		o.pauseCh = make(chan struct{})
	}
	o.m.Unlock()
	return nil
}

// ResumeNewAssignments tells the orchestrator that it may resume
// assignments of partitions to nodes, and is idempotent.
func (o *Orchestrator) ResumeNewAssignments() error {
	o.m.Lock()
	if o.pauseCh != nil {
		close(o.pauseCh)
		o.pauseCh = nil
	}
	o.m.Unlock()
	return nil // TODO.
}

func (o *Orchestrator) runTokens(numStartTokens int) {
	defer close(o.tokensSupplyCh)

	for i := 0; i < numStartTokens; i++ {
		// Tokens available to throttle concurrency.  The # of
		// outstanding tokens might be changed dynamically and can
		// also be used to synchronize with any optional, external
		// manager (i.e., ns-server wants cbft to do X number of moves
		// with M concurrency before forcing a cluster-wide
		// compaction).
		o.tokensSupplyCh <- i
	}

	for {
		select {
		case token, ok := <-o.tokensReleaseCh:
			if !ok {
				return
			}

			// Check if we're paused w.r.t. starting new reassignments.
			o.m.Lock()
			stopCh := o.stopCh
			pauseCh := o.pauseCh
			o.m.Unlock()

			if stopCh != nil {
				if pauseCh != nil {
					select {
					case <-stopCh:
						// PASS.
					case <-pauseCh:
						// We're now resumed.
						o.tokensSupplyCh <- token
					}
				} else {
					o.tokensSupplyCh <- token
				}
			}
		}
	}
}

func (o *Orchestrator) runMover(node string, stopCh chan struct{}) error {
	for {
		select {
		case _, ok := <-stopCh:
			if !ok {
				return nil
			}

		case token, ok := <-o.tokensSupplyCh:
			if !ok {
				return nil
			}

			partition, state, insertAt, fromNode, fromNodeTakeOver, err :=
				o.nextPartitionMove(node)
			if err != nil || partition == "" {
				o.tokensReleaseCh <- token
				return err
			}

			err = o.assignPartition(partition, node, state,
				insertAt, fromNode, fromNodeTakeOver)
			if err != nil {
				o.tokensReleaseCh <- token
				return err
			}

			err = o.waitForPartitionNodeState(partition,
				node, state, insertAt)
			if err != nil {
				o.tokensReleaseCh <- token
				return err
			}

			if fromNode != "" {
				err = o.unassignPartition(partition, node, state)
				if err != nil {
					o.tokensReleaseCh <- token
					return err
				}

				err = o.waitForPartitionNodeState(partition,
					node, "", -1)
				if err != nil {
					o.tokensReleaseCh <- token
					return err
				}
			}

			o.tokensReleaseCh <- token
		}
	}

	return nil
}

func (o *Orchestrator) nextPartitionMove(node string) (
	partition string,
	state string,
	insertAt int,
	fromNode string,
	fromNodeTakeOver bool,
	err error) {
	partitionMove, ok := <-o.mapNodeToPartitionMoveCh[node]
	if !ok {
		return "", "", -1, "", false, nil
	}

	return partitionMove.partition, partitionMove.state,
		-1, "", false, nil
}

func (o *Orchestrator) runPartitionMoveFeeder() {
	for {
		// The availableMoves is keyed by node name.
		availableMoves := map[string][]*nextMoves{}

		o.m.Lock()

		for _, nextMoves := range o.mapPartitionToNextMoves {
			if nextMoves.next < len(nextMoves.moves) {
				node := nextMoves.moves[nextMoves.next].Node
				availableMoves[node] =
					append(availableMoves[node], nextMoves)
			}
		}

		o.m.Unlock()

		if len(availableMoves) <= 0 {
			break
		}

		nodeFeedersStopCh := make(chan struct{})
		nodeFeedersDoneCh := make(chan bool)

		for node, nextMovesArr := range availableMoves {
			// TODO: Can optimize by finding least instead of full sort.
			sort.Sort(&nextMovesSorter{nextMovesArr})

			go func(node string, nextMoves *nextMoves) {
				o.m.Lock()
				nodeStateOp := nextMoves.moves[nextMoves.next]
				o.m.Unlock()

				partitionMove := partitionMove{
					partition: nextMoves.partition,
					state:     nodeStateOp.State,
					op:        nodeStateOp.Op,
				}

				select {
				case <-nodeFeedersStopCh:
					// NOOP.

				case o.mapNodeToPartitionMoveCh[node] <- partitionMove:
					o.m.Lock()
					nextMoves.next++
					o.m.Unlock()

					nodeFeedersDoneCh <- true
					return
				}

				nodeFeedersDoneCh <- false
			}(node, nextMovesArr[0])
		}

		stopped := false

		for range availableMoves {
			wasFed := <-nodeFeedersDoneCh
			if wasFed && !stopped {
				close(nodeFeedersStopCh)
				stopped = true
			}
		}

		if !stopped {
			close(nodeFeedersStopCh)
		}

		close(nodeFeedersDoneCh)
	}

	for _, partitionMoveCh := range o.mapNodeToPartitionMoveCh {
		close(partitionMoveCh)
	}
}

var opWeight = map[string]int{
	"promote": 0,
	"demote":  1,
	"add":     2,
	"del":     3,
}

type nextMovesSorter struct {
	s []*nextMoves
}

func (a *nextMovesSorter) Len() int {
	return len(a.s)
}

func (a *nextMovesSorter) Less(i, j int) bool {
	opi := a.s[i].moves[a.s[i].next].Op
	opj := a.s[j].moves[a.s[j].next].Op

	return opWeight[opi] < opWeight[opj]
}

func (a *nextMovesSorter) Swap(i, j int) {
	a.s[i], a.s[j] = a.s[j], a.s[i]
}

func (o *Orchestrator) waitForPartitionNodeState(
	partition string,
	node string,
	state string,
	position int) error {
	// TODO.
	return nil
}
