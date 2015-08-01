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

type Orchestrator struct {
	label string

	partitionModel PartitionModel

	options OrchestratorOptions

	destMap PartitionMap
	currMap func() (PartitionMap, error)

	assignPartition   func(partition string, node string, priority int) error
	unassignPartition func(partition string, node string) error
	partitionState    func(partition string, node string) (
		state string, pct float32, err error)

	statsCh chan OrchestratorStats
	stopCh  chan struct{}
}

// OrchestratorMoves asynchronously begins reassigning partitions
// between nodes to reach the destMap.
func OrchestrateMoves(
	label string,
	partitionModel PartitionModel,
	options OrchestratorOptions,
	destMap PartitionMap,
	currMap func() (PartitionMap, error),
	assignPartition func(partition string, node string, priority int) error,
	unassignPartition func(partition string, node string) error,
	partitionState func(partition string, node string) (
		state string, pct float32, err error),
) (*Orchestrator, error) {
	o := &Orchestrator{
		label:             label,
		partitionModel:    partitionModel,
		options:           options,
		destMap:           destMap,
		currMap:           currMap,
		assignPartition:   assignPartition,
		unassignPartition: unassignPartition,
		partitionState:    partitionState,
		statsCh:           make(chan OrchestratorStats),
		stopCh:            make(chan struct{}),
	}

	// TODO: Start goroutine.

	return o, nil
}

// Stop asynchrnously requests the orchestrator to stop and move to
// the done state.
func (o *Orchestrator) Stop() {
	close(o.stopCh)
}

// StatsCh returns a channel that is updated occassionally when the
// orchestrator has made some progress on one or more partition
// reassignments.  The channel is closed when the orchestrator is
// finished, either naturally or via a Stop(), and all the
// orchestrator's resources have been released.
func (o *Orchestrator) StatsCh() chan OrchestratorStats {
	return o.statsCh
}

// PauseNewAssignments disallows the orchestrator from starting any
// new assignments of partitions to nodes, allowing the caller to
// throttle the concurrency of moves.  Any inflight partition moves
// will continue to be finished.  The caller can monitor the StatsCh
// to determine when to pause and/or resume partition assignments.
// PauseNewAssignments is idempotent.
func (o *Orchestrator) PauseNewAssignments() error {
	return nil // TODO.
}

// ResumeNewAssignments tells the orchestrator that it may resume
// assignments of partitions to nodes, and is idempotent.
func (o *Orchestrator) ResumeNewAssignments() error {
	return nil // TODO.
}

type OrchestratorOptions struct {
	MaxConcurrentPartitionMovesPerCluster int
	MaxConcurrentPartitionBuildsPerNode   int
}

type OrchestratorStats struct {
	TotPartitionsAssigned       int
	TotPartitionsAssignedDone   int
	TotPartitionsUnassigned     int
	TotPartitionsUnassignedDone int
}

/*
----------------------------------
curr|dest:
     abcd|bcd
  00 m   |m
  01  m  |m
  02   m | m
  03    m|  m

moves:
  00 m a->b

----------------------------------
curr|dest:
     abcd|bcd
  00 m   |m
  01  m  |m
  02   m | m
  03    m|  m
  04 m   | m

moves:
  00 m a->b,
  04 m a->c.

----------------------------------
curr|dest:
     abcd|a
  00 m   |m
  01  m  |m
  02   m |m
  03    m|m
  04 m   |m

M: 1
N: 1

moves:
  01 m b->a.
  02 m c->a.
  03 m d->a.
*/
