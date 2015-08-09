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
	"errors"
	"fmt"
	"sync"
)

var ErrorStopped = errors.New("stopped")

/*
TODO: Some move prioritization heuristics to consider:

First, favor easy, single-node promotions and demotions (e.g., a
replica partition graduating to master on the same node) because
single-node state changes should be fast and so that clients can have
more coverage across all partitions.  The
LowestWeightPartitionMoveForNode() implementation does this now.

Next, favor assignments of partitions that have no replicas assigned
anywhere, where we want to get to that first PIndex instance or
replica as soon as possible. Once we have that first replica for a
PIndex, though, we should consider favoring other kinds of moves over
building even more replicas of that PIndex.

Next, favor reassignments that utilize capacity on newly added cbft
nodes, as the new nodes may be able to help with existing, overtaxed
nodes. But be aware: starting off more KV backfills may push existing
nodes running at the limit over the edge.

Next, favor reassignments that help get partitions off of nodes that are
leaving the cluster. The idea is to allow ns-server to remove
couchbase nodes (which may need servicing) sooner.

Next, favor removals of partitions that are over-replicated. For
example, there might be too many replicas of a PIndex remaining on
new/existing nodes.

Lastly, favor reassignments that move partitions amongst cbft nodes than
are neither joining nor leaving the cluster. In this case, MCP may
need to shuffle partitions to achieve better balance or meet replication
constraints.

Other, more advanced factors to consider in the heuristics, which may
be addressed in future releases, but would just be additions to the
ordering/sorting algorithm.

Some cbft nodes might be slower, less powerful and more impacted than
others.

Some partitions might be way behind compared to others.

Some partitions might be much larger than others.

Some partitions might have data sources under more pressure than
others and less able to handle yet another a request for a data source
full-scan (backfill).

Perhaps consider how about some randomness?
*/

// ------------------------------------------

// An Orchestrator instance holds the runtime state during an
// OrchestrateMoves() operation.
type Orchestrator struct {
	model PartitionModel

	options OrchestratorOptions

	nodesAll []string

	begMap PartitionMap
	endMap PartitionMap

	assignPartition AssignPartitionFunc
	partitionState  PartitionStateFunc
	findMove        FindMoveFunc

	progressCh chan OrchestratorProgress

	mapNodeToPartitionMoveCh map[string]chan PartitionMove

	m sync.Mutex // Protects the fields that follow.

	stopCh   chan struct{} // Becomes nil when stopped.
	pauseCh  chan struct{} // May be nil; non-nil when paused.
	progress OrchestratorProgress

	mapPartitionToNextMoves map[string]*nextMoves
}

// OrchestratorOptions represents advanced config parameters for
// OrchestrateMoves().
type OrchestratorOptions struct {
	MaxConcurrentPartitionMovesPerNode int
}

// OrchestratorProgress represents progress counters and/or error
// information as the OrchestrateMoves() operation proceeds.
type OrchestratorProgress struct {
	Errors                      []error
	TotPartitionsAssigned       int
	TotPartitionsAssignedDone   int
	TotPartitionsUnassigned     int
	TotPartitionsUnassignedDone int
	TotRunMover                 int
	TotRunMoverLoop             int
	TotRunMoverDone             int
	TotRunMoverDoneErr          int
	TotRunSupplyMovesLoop       int
	TotRunSupplyMovesDone       int
	TotRunSupplyMovesPause      int
	TotRunSupplyMovesResume     int
	TotStop                     int
	TotPauseNewAssignments      int
	TotResumeNewAssignments     int
}

// AssignPartitionFunc is a callback invoked by OrchestrateMoves()
// when it wants to asynchronously assign a partition to a node at a
// given state, or change the state of an existing partition on a
// node.  The state will be "" if the partition should be removed or
// deleted from the ndoe.
type AssignPartitionFunc func(stopCh chan struct{},
	partition string,
	node string,
	state string,
	op string) error

// PartitionStateFunc is a callback invoked by OrchestrateMoves()
// when it wants to synchronously retrieve information about a
// partition on a node.
type PartitionStateFunc func(stopCh chan struct{},
	partition string, node string) (
	state string, pct float32, err error)

// FindMoveFunc is a callback invoked by OrchestrateMoves() when it
// wants to find the best partition move out of a set of available
// partition moves for node.  It should return the array index of the
// partition move that should be used next.
type FindMoveFunc func(node string, moves []PartitionMove) int

// A PartitionMove struct represents a state change or operation on a
// partition on a node.
type PartitionMove struct {
	Partition string

	Node string

	// Ex: "master", "replica".
	State string

	// Same as NodeStateOp.Op: "add", "del", "promote", "demote".
	Op string
}

// LowestWeightPartitionMoveForNode implements the FindMoveFunc
// callback signature, by using the MoveOpWeight lookup table to find
// the lowest weight partition move for a node.
func LowestWeightPartitionMoveForNode(
	node string, moves []PartitionMove) int {
	r := 0
	for i, move := range moves {
		if MoveOpWeight[moves[r].Op] > MoveOpWeight[move.Op] {
			r = i
		}
	}
	return r
}

var MoveOpWeight = map[string]int{
	"promote": 1,
	"demote":  2,
	"add":     3,
	"del":     4,
}

// ------------------------------------------

// A nextMoves struct is used to track a sequence of moves of a
// partition, including the next move that that needs to be taken.
type nextMoves struct {
	partition string // Immutable.

	// Mutable index or current position in the moves array that
	// represents the next available move for a partition.
	next int

	// The sequence of moves can come from the output of the
	// CalcPartitionMoves() function and is immutable.
	moves []NodeStateOp
}

// ------------------------------------------

// OrchestratorMoves asynchronously begins reassigning partitions
// amongst nodes in order to transition from the begMap to the endMap
// state, invoking the assignPartition() to affect changes.
// Additionally, the caller must read the progress channel until it's
// closed by OrchestrateMoves to avoid blocking the orchestration, and
// as a way to monitor progress.
//
// The nodesAll must be a union or superset of all the nodes during
// the orchestration (nodes added, removed, unchanged).
//
// The findMove callback is invoked when OrchestrateMoves needs to
// find the best move for a node from amongst a set of available
// moves.
func OrchestrateMoves(
	model PartitionModel,
	options OrchestratorOptions,
	nodesAll []string,
	begMap PartitionMap,
	endMap PartitionMap,
	assignPartition AssignPartitionFunc,
	partitionState PartitionStateFunc,
	findMove FindMoveFunc,
) (*Orchestrator, error) {
	if len(begMap) != len(endMap) {
		return nil, fmt.Errorf("mismatched begMap and endMap")
	}

	// Populate the mapNodeToPartitionMoveCh, keyed by node name.
	mapNodeToPartitionMoveCh := map[string]chan PartitionMove{}
	for _, node := range nodesAll {
		mapNodeToPartitionMoveCh[node] = make(chan PartitionMove)
	}

	states := sortStateNames(model)

	// Populate the mapPartitionToNextMoves, keyed by partition name,
	// with the output from CalcPartitionMoves().
	//
	// As an analogy, this step calculates a bunch of airplane flight
	// plans, without consideration to what the other airplanes are
	// doing, where each flight plan has multi-city, multi-leg hops.
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
		model:           model,
		options:         options,
		nodesAll:        nodesAll,
		begMap:          begMap,
		endMap:          endMap,
		assignPartition: assignPartition,
		partitionState:  partitionState,
		findMove:        findMove,
		progressCh:      make(chan OrchestratorProgress),

		mapNodeToPartitionMoveCh: mapNodeToPartitionMoveCh,

		stopCh:  make(chan struct{}),
		pauseCh: nil,

		mapPartitionToNextMoves: mapPartitionToNextMoves,
	}

	stopCh := o.stopCh

	runMoverDoneCh := make(chan error)

	// Start concurrent movers.
	//
	// Following the airplane analogy, a runMover() represents
	// a takeoff runway at a city airport (or node).  There can
	// be multiple takeoff runways at a city's airport (which is
	// controlled by MaxConcurrentPartitionMovesPerNode).
	m := options.MaxConcurrentPartitionMovesPerNode
	if m < 1 {
		m = 1
	}

	for _, node := range o.nodesAll {
		for i := 0; i < m; i++ {
			go func(node string) {
				o.m.Lock()
				o.progress.TotRunMover++
				progress := o.progress
				o.m.Unlock()

				o.progressCh <- progress

				// The partitionMoveCh has commands from the global,
				// supreme airport controller on which airplane (or
				// partition) should takeoff from the city airport
				// next (but the supreme airport controller doesn't
				// care which takeoff runway is used).
				partitionMoveCh := o.mapNodeToPartitionMoveCh[node]

				runMoverDoneCh <- o.runMover(stopCh, partitionMoveCh, node)
			}(node)
		}
	}

	// Supply moves to movers.
	//
	// Following the airplane/airport analogy, a runSupplyMoves()
	// goroutine is like some global, supreme airport controller,
	// remotely controlling all the city airports across the entire
	// realm, and deciding which plane can take off next at each
	// airport.  Each plane is following its multi-leg flight plan
	// that was computed from earlier (via CalcPartitionMoves), but
	// when multiple planes are concurrently ready to takeoff from a
	// city's airport (or node), this global, supreme airport
	// controller chooses which plane (or partition) gets to takeoff
	// next.
	go o.runSupplyMoves(stopCh)

	// Wait for movers to finish and then cleanup.
	go func() {
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
		o.progress.TotStop++
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
		o.progress.TotPauseNewAssignments++
	}
	o.m.Unlock()
	return nil
}

// ResumeNewAssignments tells the orchestrator that it may resume
// assignments of partitions to nodes, and is idempotent.
func (o *Orchestrator) ResumeNewAssignments() error {
	o.m.Lock()
	if o.pauseCh != nil {
		o.progress.TotResumeNewAssignments++
		close(o.pauseCh)
		o.pauseCh = nil
	}
	o.m.Unlock()
	return nil // TODO.
}

func (o *Orchestrator) runMover(stopCh chan struct{},
	partitionMoveCh chan PartitionMove, node string) error {
	for {
		o.m.Lock()
		o.progress.TotRunMoverLoop++
		o.m.Unlock()

		select {
		case _, ok := <-stopCh:
			if !ok {
				return nil
			}

		case partitionMove, ok := <-partitionMoveCh:
			if !ok {
				return nil
			}

			partition := partitionMove.Partition
			if partition == "" {
				return nil
			}

			state := partitionMove.State

			err := o.assignPartition(stopCh,
				partition, node, state, partitionMove.Op)
			if err != nil {
				return err
			}

			err = o.waitForPartitionNodeState(stopCh,
				partition, node, state)
			if err != nil {
				return err
			}
		}
	}
}

func (o *Orchestrator) runSupplyMoves(stopCh chan struct{}) {
	keepGoing := true

	for keepGoing {
		// The availableMoves is keyed by node name.
		availableMoves := map[string][]*nextMoves{}

		o.m.Lock()

		o.progress.TotRunSupplyMovesLoop++

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
			o.m.Lock()
			o.progress.TotRunSupplyMovesPause++
			o.m.Unlock()

			<-pauseCh

			o.m.Lock()
			o.progress.TotRunSupplyMovesResume++
			o.m.Unlock()
		}

		nodeFeedersStopCh := make(chan struct{})
		nodeFeedersDoneCh := make(chan bool)

		// Broadcast to every node mover their next, best move, and
		// when the one or more node movers is successfully "fed",
		// then stop the broadcast (via nodeFeedersStopCh) so that we
		// can repeat the outer loop to re-calculate available moves.
		for node, nextMovesArr := range availableMoves {
			go func(node string, nextMoves *nextMoves) {
				o.m.Lock()
				nodeStateOp := nextMoves.moves[nextMoves.next]
				o.m.Unlock()

				partitionMove := PartitionMove{
					Partition: nextMoves.partition,
					Node:      nodeStateOp.Node,
					State:     nodeStateOp.State,
					Op:        nodeStateOp.Op,
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
			}(node, o.findNextMoves(node, nextMovesArr))
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

	o.m.Lock()
	o.progress.TotRunSupplyMovesDone++
	o.m.Unlock()
}

func (o *Orchestrator) findNextMoves(
	node string, nextMovesArr []*nextMoves) *nextMoves {
	moves := make([]PartitionMove, len(nextMovesArr))

	for i, nextMoves := range nextMovesArr {
		m := nextMoves.moves[nextMoves.next]

		moves[i] = PartitionMove{
			Partition: nextMoves.partition,
			Node:      m.Node,
			State:     m.State,
			Op:        m.Op,
		}
	}

	return nextMovesArr[o.findMove(node, moves)]
}

func (o *Orchestrator) waitForPartitionNodeState(
	stopCh chan struct{},
	partition string,
	node string,
	state string) error {
	for {
		select {
		case <-stopCh:
			return ErrorStopped
		default:
		}

		currState, currPct, err :=
			o.partitionState(stopCh, partition, node)
		if err != nil {
			return err
		}

		if currState == state && currPct >= 0.99 {
			return nil
		}
	}
}
