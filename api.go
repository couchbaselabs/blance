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

// Package blance provides a partition assignment library, using a
// greedy, heuristic, functional approach.  It supports multiple,
// user-configurable partition states (master, replica, read-only,
// etc), multi-level containment hierarchy (shelf/rack/zone/datacenter
// awareness) with configurable inclusion/exclusion policies,
// heterogeneous partition weights, heterogeneous node weights,
// partition stickiness control, and multi-master support.
package blance

// A PartitionMap represents all the partitions for some logical
// resource, where the partitions are assigned to different nodes and
// with different states.  For example, partition "A...H" is assigned
// to node "x" as a "master" and to node "y" as a "replica".  And,
// partition "I...Z" is assigned node "y" as a "master" and to nodes
// "z" & "x" as "replica".
type PartitionMap map[string]*Partition // Keyed by Partition.Name.

// A Partition represents a distinct, non-overlapping subset (or a
// shard) of some logical resource.
type Partition struct {
	// The Name of a Partition must be unique within a PartitionMap.
	Name string

	// NodesByState is keyed is stateName, and value is an array of
	// node names.  For example, {"master": ["a"], "replica": ["b",
	// "c"]}.
	NodesByState map[string][]string
}

// A PartitionModel lets applications define different states for each
// partition per node, such as "master", "slave", "dead", etc.  Key is
// stateName, like "master", "slave", "dead", etc.
type PartitionModel map[string]*PartitionModelState

// A PartitionModelState lets applications define metadata per
// partition model state.  For example, "master" state should have
// different priority and constraints than a "slave" state.
type PartitionModelState struct {
	// Priority of zero is the highest.  e.g., "master" Priority
	// should be < than "slave" Priority, so we can define that
	// as "master" Priority of 0 and "slave" priority of 1.
	Priority int

	// A Constraint defines how many nodes the algorithm strives to
	// assign a partition.  For example, for any given partition,
	// perhaps the application wants 1 node to have "master" state and
	// wants 2 nodes to have "slave" state.  That is, the "master"
	// state has Contraints of 1, and the "slave" state has
	// Constraints of 2.  Continuing the example, when the "master"
	// state has Priority of 0 and the "slave" state has Priority of
	// 1, then "master" partitions will be assigned to nodes before
	// "slave" partitions.
	Constraints int
}

// HierarchyRules example:
// {"slave":[{IncludeLevel:1,ExcludeLevel:0}]}, which means that after
// a partition is assigned to a node as master, then assign the first
// slave to a node that is a close sibling node to the master node
// (e.g., same parent or same rack).  Another example:
// {"slave":[{IncludeLevel:1,ExcludeLevel:0},
// {IncludeLevel:2,ExcludeLevel:1}]}, which means assign the first
// slave same as above, but assign the second slave to a node that is
// not a sibling of the master (not the same parent, so to a different
// rack).
type HierarchyRules map[string][]*HierarchyRule

// A HierarchyRule is metadata for rack/zone awareness features.
// First, IncludeLevel is processed to find a set of candidate nodes.
// Then, ExcludeLevel is processed to remove or exclude nodes from
// that set.  For example, for this containment tree, (datacenter0
// (rack0 (nodeA nodeB)) (rack1 (nodeC nodeD))), lets focus on nodeA.
// If IncludeLevel is 1, that means go up 1 parent (so, from nodeA up
// to rack0) and then take all of rack0's leaves: nodeA and nodeB.
// So, the candidate nodes of nodeA and nodeB are all on the same rack
// as nodeA, or a "same rack" policy.  If instead the IncludeLevel was
// 2 and ExcludeLevel was 1, then that means a "different rack"
// policy.  With IncludeLevel of 2, we go up 2 ancestors from node A
// (from nodeA to rack0; and then from rack0 to datacenter0) to get to
// datacenter0.  The datacenter0 has leaves of nodeA, nodeB, nodeC,
// nodeD, so those nodes comprise the inclusion candidate set.  But,
// with ExcludeLevel of 1, that means we go up 1 parent from nodeA to
// rack0, take rack0's leaves, giving us an exclusion set of nodeA &
// nodeB.  The inclusion candidate set minus the exclusion set finally
// gives us just nodeC & nodeD as our final candidate nodes.  That
// final candidate set of nodes (just nodeC & nodeD) are from a
// different rack as nodeA.
type HierarchyRule struct {
	// IncludeLevel defines how many parents or ancestors to traverse
	// upwards in a containment hierarchy to find candidate nodes.
	IncludeLevel int

	// ExcludeLevel defines how many parents or ancestors to traverse
	// upwards in a containment hierarchy to find an exclusion set of
	// nodes.
	ExcludeLevel int
}

// PlanNextMap is the main entry point to the algorithm to assign
// partitions to nodes.  The prevMap must at least define the
// partitions.  Partitions must be stable between PlanNextMap() runs.
// That is, splitting and merging or partitions are an orthogonal
// concern and must be done separately than PlanNextMap() invocations.
// The nodeAll parameters is all nodes (union of existing nodes, nodes
// to be added, nodes to be removed, nodes that aren't changing).  The
// nodesToRemove may be empty.  The nodesToAdd may be empty.  When
// both nodesToRemove and nodesToAdd are empty, partitioning
// assignment may still change, as another PlanNextMap() invocation
// may reach more stabilization or balanced'ness.  The model is
// required.  The modelStateContraints is optional, and allows the
// caller to override the contrainsts defined in the model.  The
// modelStateContrains is keyed by stateName (like "master", "slave",
// etc).  The partitionWeights is optional and is keyed by
// partitionName; it allows the caller to specify that some partitions
// are bigger than others (e.g., California has more records than
// Hawaii); default partitionWeight is 1.  The stateStickiness is
// optional and is keyed by stateName; it allows the caller to prefer
// not moving data at the tradeoff of potentially more imbalance;
// default stateStickiness is 1.5.  The nodeWeights is optional and is
// keyed by node name; it allows the caller to specify that some nodes
// can hold more partitions than other nodes; default nodeWeight is 1.
// The nodeHierarchy is optional; it defines the parent relationships
// per node; it is keyed by node and a value is the node's parent.
// The hierarchyRules is optional and allows the caller to define
// slave placement policy (e.g., same/different rack; same/different
// zone; etc).
func PlanNextMap(
	prevMap PartitionMap,
	nodesAll []string, // Union of nodesToRemove, nodesToAdd and non-changing nodes.
	nodesToRemove []string,
	nodesToAdd []string,
	model PartitionModel,
	modelStateConstraints map[string]int, // Keyed by stateName.
	partitionWeights map[string]int, // Keyed by partitionName.
	stateStickiness map[string]int, // Keyed by stateName.
	nodeWeights map[string]int, // Keyed by node.
	nodeHierarchy map[string]string, // Keyed by node, value is node's parent.
	hierarchyRules HierarchyRules,
) (nextMap PartitionMap, warnings []string) {
	return planNextMap(prevMap,
		nodesAll, nodesToRemove, nodesToAdd,
		model, modelStateConstraints,
		partitionWeights,
		stateStickiness,
		nodeWeights,
		nodeHierarchy,
		hierarchyRules)
}
