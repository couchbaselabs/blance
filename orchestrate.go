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
	"sync"
)

var ErrorStopped = errors.New("stopped")

/*
TODO: Some move prioritization heuristics to consider:

First, favor easy promotions and demotions (e.g., a replica graduating
to master) so that clients can have more coverage across all
partitions. This is the equivalent of a VBucket changing state from
replica to master.

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
type PartitionStateFunc func(stopCh chan struct{},
	partition string, node string) (
	state string, pct float32, err error)

// ------------------------------------------

// A partitionMove struct represents a requested state assignment of a
// partition on a node.
type partitionMove struct {
	partition string

	// Ex: "master", "replica".
	state string

	// Same as NodeStateOp.Op: "add", "del", "promote", "demote".
	op string
}

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
func OrchestrateMoves(
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
