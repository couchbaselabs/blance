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

// An Orchestrator instance holds the runtime state during an
// OrchestrateMoves() operation.
type Orchestrator struct {
	label string

	partitionModel PartitionModel

	options OrchestratorOptions

	nodesAll []string

	destMap PartitionMap
	currMap func() (PartitionMap, error)

	assignPartition   AssignPartitionFunc
	unassignPartition UnassignPartitionFunc
	partitionState    PartitionStateFunc

	progressCh chan OrchestratorProgress
	doneCh     chan struct{}

	tokensSupplyCh  chan int
	tokensReleaseCh chan int
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

// OrchestratorMoves asynchronously begins reassigning partitions
// amongst nodes to reach the destMap state, invoking the callback
// functions like assignPartition() and unassignPartition() affect
// changes.
func OrchestrateMoves(
	label string,
	partitionModel PartitionModel,
	options OrchestratorOptions,
	nodesAll []string,
	destMap PartitionMap,
	currMap func() (PartitionMap, error),
	assignPartition AssignPartitionFunc,
	unassignPartition UnassignPartitionFunc,
	partitionState PartitionStateFunc,
) (*Orchestrator, error) {
	o := &Orchestrator{
		label:             label,
		partitionModel:    partitionModel,
		options:           options,
		nodesAll:          nodesAll,
		destMap:           destMap,
		currMap:           currMap,
		assignPartition:   assignPartition,
		unassignPartition: unassignPartition,
		partitionState:    partitionState,
		progressCh:        make(chan OrchestratorProgress),
		doneCh:            make(chan struct{}),
		tokensSupplyCh:    make(chan int),
		tokensReleaseCh:   make(chan int),
	}

	go o.runTokens()

	go o.runNodes()

	return o, nil
}

// Stop asynchronously requests the orchestrator to stop, where the
// caller will eventually see a closed progress channel.
func (o *Orchestrator) Stop() {
	close(o.doneCh)
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

func (o *Orchestrator) runTokens() {
	defer close(o.tokensSupplyCh)

	m := o.options.MaxConcurrentPartitionMovesPerCluster
	for i := 0; i < m; i++ {
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
		case _, ok := <-o.doneCh:
			if !ok {
				return
			}

		case token, ok := <-o.tokensReleaseCh:
			if !ok {
				return
			}

			o.tokensSupplyCh <- token
		}
	}
}

func (o *Orchestrator) runNodes() {
	nodesDoneCh := make(chan error)

	for _, node := range o.nodesAll {
		go func() {
			nodesDoneCh <- o.runNode(node)
		}()
	}

	for range o.nodesAll {
		<-nodesDoneCh
	}

	close(o.tokensReleaseCh)

	close(o.progressCh)
}

func (o *Orchestrator) runNode(node string) error {
	for {
		select {
		case _, ok := <-o.doneCh:
			if !ok {
				return nil
			}

		case token, ok := <-o.tokensSupplyCh:
			if !ok {
				return nil
			}

			partition, state, insertAt, fromNode, fromNodeTakeOver, err :=
				o.calcNextPartitionToAssignToNode(node)
			if err != nil {
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

func (o *Orchestrator) calcNextPartitionToAssignToNode(node string) (
	partition string,
	state string,
	insertAt int,
	fromNode string,
	fromNodeTakeOver bool,
	err error) {
	// TODO.
	return "", "", 0, "", false, nil
}

func (o *Orchestrator) waitForPartitionNodeState(
	partition string,
	node string,
	state string,
	position int) error {
	// TODO.
	return nil
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
