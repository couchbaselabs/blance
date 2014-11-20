//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// The blance package provides a partition rebalancing algorithm,
// using a greedy, single-pass, heuristic, functional approach.
package blance

type PartitionMap struct {
	Partitions map[string]*Partition // Keyed by Partition.Name.
}

type Partition struct {
	// Name must be unique within a PartitionMap.Partitions.
	Name string

	// Key is stateName, value is array of node names.  For example,
	// {"master": ["a"], "replica": ["b", "c"]}.
	NodesByState map[string][]string
}

// A PartitionModel lets applications define different states for each
// partition per node, such as "master", "slave", "dead", etc.
type PartitionModel struct {
	// Keyed by stateName, like "master", "slave", "dead", etc.
	States map[string]*PartitionModelState
}

// A PartitionModelState lets applications define metadata per
// partition model state.  For example, "master" state should have
// different constraints than a "slave" state.
type PartitionModelState struct {
	// Priority of zero is the highest.  e.g., "master" Priority
	// should be < than "slave" Priority.
	Priority    int
	Constraints int
}

func RebalancePartitions(
	prevMap *PartitionMap,
	nodesToRemove []string,
	nodesToAdd []string,
	model *PartitionModel,
	// Keyed by same key as the key to partitionModel.States, e.g.,
	// "master", "slave", "dead", etc.
	modelStateConstraints map[string]int,
	partitionWeights map[string]int) *PartitionMap {
	return rebalancePartitions(prevMap, nodesToRemove, nodesToAdd,
		model, modelStateConstraints, partitionWeights)
}
