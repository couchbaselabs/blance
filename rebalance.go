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
// using a greedy, single-pass, heuristic approach.
package blance

import (
	"fmt"
	"sort"
)

func rebalancePartitions(
	prevMap PartitionMap,
	nodesToRemove []string,
	nodesToAdd []string,
	model *PartitionModel,
	// Keyed by same key as the key to partitionModel.States, e.g.,
	// "master", "slave", "dead", etc.
	modelStateConstraints map[string]int,
	partitionWeights map[string]int,
) PartitionMap {
	// Start by filling out nextPartitions as a deep clone of
	// prevMap.Partitions, but filter out the to-be-removed nodes.
	nextPartitions := prevMap.toArrayCopy()
	for _, partition := range nextPartitions {
		partition.NodesByState =
			removeNodesFromNodesByState(partition.NodesByState, nodesToRemove)
	}
	sort.Sort(&partitionSorter{a: nextPartitions})

	// Given a PartitionModel state name and its constraints, for
	// every partition, assign nodes.
	assignStateToPartitions := func(stateName string, constraints int) {
		// Sort the partitions to help reach a better assignment.
		p := &partitionSorter{
			stateName:        stateName,
			prevPartitions:   prevMap,
			nodesToRemove:    nodesToRemove,
			nodesToAdd:       nodesToAdd,
			partitionWeights: partitionWeights,
			a:                append([]*Partition(nil), nextPartitions...),
		}
		sort.Sort(p)

		// TODO: complete the rest of the algorithm.
	}

	// Run through the sorted partition states (master, slave, etc)
	// that have constraints and invoke assignStateToPartitions().
	pms := &stateNameSorter{
		m: model,
		s: make([]string, 0, len(model.States)),
	}
	for stateName, _ := range model.States {
		pms.s = append(pms.s, stateName)
	}
	sort.Sort(pms)
	for _, stateName := range pms.s {
		constraints, exists := modelStateConstraints[stateName]
		if !exists {
			modelState, exists := model.States[stateName]
			if exists && modelState != nil {
				constraints = modelState.Constraints
			}
		}
		if constraints > 0 {
			assignStateToPartitions(stateName, constraints)
		}
	}

	return nil // TODO: return a real PartitionMap
}

// Makes a deep copy of the PartitionMap as an array.
func (m PartitionMap) toArrayCopy() []*Partition {
	rv := make([]*Partition, 0, len(m))
	for _, partition := range m {
		rv = append(rv, &Partition{
			Name:         partition.Name,
			NodesByState: copyNodesByState(partition.NodesByState),
		})
	}
	return rv
}

func copyNodesByState(nbs map[string][]string) map[string][]string {
	rv := make(map[string][]string)
	for stateName, nodes := range nbs {
		rv[stateName] = append([]string(nil), nodes...)
	}
	return rv
}

// --------------------------------------------------------

// Returns a copy of nodesByState but with nodes removed.
// Example, when removeNodes == ["a"] and
// nodesByState == {"master": ["a"], "slave": ["b"]},
// then result will be {"master": [], "slave": ["b"]}.
func removeNodesFromNodesByState(
	nodesByState map[string][]string,
	removeNodes []string,
) map[string][]string {
	rv := make(map[string][]string)
	for stateName, nodes := range nodesByState {
		rv[stateName] = StringsRemoveStrings(nodes, removeNodes)
	}
	return rv
}

// Given a nodesByState, like {"master": ["a"], "slave": ["b", "c"]},
// this function might return something like ["b", "c", "a"].
func flattenNodesByState(nodesByState map[string][]string) []string {
	rv := make([]string, 0)
	for _, nodes := range nodesByState {
		rv = append(rv, nodes...)
	}
	return rv
}

// --------------------------------------------------------

// Does ORDER BY m.States[stateName].Priority ASC, stateName ASC".
type stateNameSorter struct {
	m *PartitionModel
	s []string // Mutated array of partition model state names.
}

func (pms *stateNameSorter) Len() int {
	return len(pms.s)
}

func (pms *stateNameSorter) Less(i, j int) bool {
	return pms.m.States[pms.s[i]].Priority < pms.m.States[pms.s[j]].Priority ||
		pms.s[i] < pms.s[j]
}

func (pms *stateNameSorter) Swap(i, j int) {
	pms.s[i], pms.s[j] = pms.s[j], pms.s[i]
}

// --------------------------------------------------------

// Does ORDER BY partitions-on-nodes-to-be-removed, then by
// partitions-who-haven't-been-assigned-anywhere-yet, then by
// partition-weight, then by partition-name.
type partitionSorter struct {
	stateName        string                // When "", just sort by partition name.
	prevPartitions   map[string]*Partition // Keyed by partition name.
	nodesToRemove    []string
	nodesToAdd       []string
	partitionWeights map[string]int // Keyed by partition name.

	a []*Partition // Mutated during sort.
}

func (r *partitionSorter) Len() int {
	return len(r.a)
}

func (r *partitionSorter) Less(i, j int) bool {
	ei := r.Entry(i)
	ej := r.Entry(j)
	for x := 0; x < len(ei) && x < len(ej); x++ {
		if ei[x] < ej[x] {
			return true
		}
	}
	return len(ei) < len(ej)
}

func (r *partitionSorter) Swap(i, j int) {
	r.a[i], r.a[j] = r.a[j], r.a[i]
}

func (r *partitionSorter) Entry(i int) []string {
	partitionName := r.a[i].Name

	// Calculate space padded (for sortability) partition weight.
	partitionWeight := 1
	if r.partitionWeights != nil {
		if w, exists := r.partitionWeights[partitionName]; exists {
			partitionWeight = w
		}
	}
	partitionWeightStr := fmt.Sprintf("%20d", partitionWeight)

	// First, favor partitions on nodes that are to-be-removed.
	if r.prevPartitions != nil &&
		r.nodesToRemove != nil {
		lastPartition := r.prevPartitions[partitionName]
		lastPartitionNBS := lastPartition.NodesByState[r.stateName]
		if lastPartitionNBS != nil &&
			len(StringsIntersectStrings(lastPartitionNBS, r.nodesToRemove)) > 0 {
			return []string{"0", partitionWeightStr, partitionName}
		}
	}

	// Then, favor partitions who haven't yet been assigned to any
	// newly added nodes yet for any state.
	if r.nodesToAdd != nil {
		fnbs := flattenNodesByState(r.a[i].NodesByState)
		if len(StringsIntersectStrings(fnbs, r.nodesToAdd)) <= 0 {
			return []string{"1", partitionWeightStr, partitionName}
		}
	}

	return []string{"2", partitionWeightStr, partitionName}
}
