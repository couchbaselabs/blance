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

	progressCh chan OrchestratorProgress
	controlCh  chan string
}

// OrchestratorMoves asynchronously begins reassigning partitions
// amongst nodes to reach the destMap state, invoking the callback
// functions like assignPartition() and unassignPartition() affect
// changes.
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
		progressCh:        make(chan OrchestratorProgress),
		controlCh:         make(chan string),
	}

	go o.run()

	return o, nil
}

// Stop asynchronously requests the orchestrator to stop, where the
// caller will eventually see a closed progress channel.
func (o *Orchestrator) Stop() {
	close(o.controlCh)
}

// ProgressCh returns a channel that is updated occassionally when the
// orchestrator has made some progress on one or more partition
// reassignments, or has reached an error.  The channel is closed by
// the orchestrator when it is finished, either naturally, or due to
// an error, or via a Stop(), and all the orchestrator's resources
// have been released.
func (o *Orchestrator) ProgressCh() chan OrchestratorProgress {
	return o.progressCh
}

// PauseNewAssignments disallows the orchestrator from starting any
// new assignments of partitions to nodes.  Any inflight partition
// moves will continue to be finished.  The caller can monitor the
// ProgressCh to determine when to pause and/or resume partition
// assignments.  PauseNewAssignments is idempotent.
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

type OrchestratorProgress struct {
	Error                       error
	TotPartitionsAssigned       int
	TotPartitionsAssignedDone   int
	TotPartitionsUnassigned     int
	TotPartitionsUnassignedDone int
}

func (o *Orchestrator) run() {
	// TODO.
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
