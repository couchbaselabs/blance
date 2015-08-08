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

	assignPartition AssignPartitionFunc
	partitionState  PartitionStateFunc

	progressCh chan OrchestratorProgress

	mapNodeToPartitionMoveCh map[string]chan partitionMove

	m sync.Mutex // Protects the fields that follow.

	stopCh   chan struct{} // Becomes nil when stopped.
	pauseCh  chan struct{} // May be nil; non-nil when paused.
	progress OrchestratorProgress

	mapPartitionToNextMoves map[string]*nextMoves
}

type OrchestratorOptions struct {
	MaxConcurrentPartitionMovesPerNode int
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
// when it wants to asynchronously assign a partition to a node at a
// given state, or change the state of an existing partition on a
// node.  The state will be "" if the partition should be removed or
// deleted from the ndoe.
type AssignPartitionFunc func(
	partition string,
	node string,
	state string,
	op string) error

// PartitionStateFunc is a callback invoked by OrchestrateMoves()
// when it wants to synchronously retrieve information about a
// partition on a node.
type PartitionStateFunc func(partition string, node string) (
	state string, pct float32, err error)

// ------------------------------------------

type partitionMove struct {
	partition string

	// Ex: "master", "replica".
	state string

	// Same as NodeStateOp.Op: "add", "del", "promote", "demote".
	op string
}

type nextMoves struct {
	partition string // Immutable.

	// Mutable index or current position in the moves array that
	// represents the next available move for a partition.
	next int

	moves []NodeStateOp // Immutable.
}

// ------------------------------------------

// OrchestratorMoves asynchronously begins reassigning partitions
// amongst nodes to transition from the begMap to the endMap state,
// invoking the assignPartition() to affect changes.  Additionally,
// the caller must read the progress channel until it's closed by
// OrchestrateMoves to avoid blocking the orchestration.
func OrchestrateMoves(
	label string,
	model PartitionModel,
	options OrchestratorOptions,
	nodesAll []string,
	begMap PartitionMap,
	endMap PartitionMap,
	assignPartition AssignPartitionFunc,
	partitionState PartitionStateFunc,
) (*Orchestrator, error) {
	m := options.MaxConcurrentPartitionMovesPerNode
	if m < 1 {
		m = 1
	}

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
		label:           label,
		model:           model,
		options:         options,
		nodesAll:        nodesAll,
		begMap:          begMap,
		endMap:          endMap,
		assignPartition: assignPartition,
		partitionState:  partitionState,
		progressCh:      make(chan OrchestratorProgress),

		mapNodeToPartitionMoveCh: mapNodeToPartitionMoveCh,

		stopCh:  make(chan struct{}),
		pauseCh: nil,

		mapPartitionToNextMoves: mapPartitionToNextMoves,
	}

	stopCh := o.stopCh

	runMoverDoneCh := make(chan error)

	// Start concurrent movers.
	for _, node := range o.nodesAll {
		for i := 0; i < m; i++ {
			go func(node string) {
				o.m.Lock()
				o.progress.TotRunMover++
				progress := o.progress
				o.m.Unlock()

				o.progressCh <- progress

				runMoverDoneCh <- o.runMover(node, stopCh)
			}(node)
		}
	}

	// Supply moves to movers.
	go o.runSupplyMoves(stopCh)

	go func() { // Wait for movers to finish and then cleanup.
		for i := 0; i < len(o.nodesAll)*m; i++ {
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

func (o *Orchestrator) runMover(node string, stopCh chan struct{}) error {
	partitionMoveCh := o.mapNodeToPartitionMoveCh[node]

	for {
		select {
		case _, ok := <-stopCh:
			if !ok {
				return nil
			}

		case partitionMove, ok := <-partitionMoveCh:
			if !ok {
				return nil
			}

			partition := partitionMove.partition
			if partition == "" {
				return nil
			}

			state := partitionMove.state

			err := o.assignPartition(partition, node, state, partitionMove.op)
			if err != nil {
				return err
			}

			err = o.waitForPartitionNodeState(stopCh, partition, node, state)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *Orchestrator) runSupplyMoves(stopCh chan struct{}) {
	keepGoing := true

	for keepGoing {
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

		pauseCh := o.pauseCh

		o.m.Unlock()

		if len(availableMoves) <= 0 {
			break
		}

		if pauseCh != nil {
			<-pauseCh
		}

		nodeFeedersStopCh := make(chan struct{})
		nodeFeedersDoneCh := make(chan bool)

		for node, nextMovesArr := range availableMoves {
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
				case <-stopCh:
					o.m.Lock()
					keepGoing = false
					o.m.Unlock()

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
			}(node, findBestNextMoves(nextMovesArr))
		}

		nodeFeedersStopChClosed := false

		for range availableMoves {
			wasFed := <-nodeFeedersDoneCh
			if wasFed && !nodeFeedersStopChClosed {
				close(nodeFeedersStopCh)
				nodeFeedersStopChClosed = true
			}
		}

		if !nodeFeedersStopChClosed {
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

func findBestNextMoves(nextMovesArr []*nextMoves) *nextMoves {
	// Simple implementation right now just favors promotions, then
	// demotions, then adds, then dels (removals).
	r := nextMovesArr[0]
	for _, x := range nextMovesArr {
		if opWeight[r.moves[r.next].Op] >
			opWeight[x.moves[x.next].Op] {
			r = x
		}
	}
	return r
}

func (o *Orchestrator) waitForPartitionNodeState(
	stopCh chan struct{},
	partition string,
	node string,
	state string) error {
	// TODO.
	return nil
}
