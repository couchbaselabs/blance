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

// NodeState associates a node with a state.  An array of NodeStates
// could be interpreted as a series of node-by-node state transitions
// for a partition.  For example, for partition X, first set node A to
// "master", then set node B to "replica", then remove partition X
// from node C (where its state will be "").
type NodeState struct {
	Node  string
	State string
}

// CalcPartitionMoves computes the step-by-step moves need to
// transition a partition from begNodesByState to endNodesByState.
//
// The states is an array of state names, like ["primary",
// "hotStandBy", "coldStandBy"], and should be ordered by more
// superior states coming earlier.  For example, "master" should come
// before "replica".
//
// The begNodesByState and endNodesByState are keyed by stateName,
// where the values are an array of node names.  For example,
// {"master": ["a"], "replica": ["b", "c"]}.
func CalcPartitionMoves(
	states []string,
	begNodesByState map[string][]string,
	endNodesByState map[string][]string,
) []NodeState {
	var moves []NodeState

	seen := map[string]bool{}

	addMoves := func(nodes []string, state string) {
		for _, node := range nodes {
			if !seen[node] {
				seen[node] = true
				moves = append(moves, NodeState{node, state})
			}
		}
	}

	begNodes := flattenNodesByState(begNodesByState)
	endNodes := flattenNodesByState(endNodesByState)

	adds := StringsRemoveStrings(endNodes, begNodes)
	subs := StringsRemoveStrings(begNodes, endNodes)

	for statei, state := range states {
		// Handle demotions of superiorTo(state) to state.
		addMoves(findStateChanges(0, statei,
			state, states, begNodesByState, endNodesByState), state)

		// Handle promotions of inferiorTo(state) to state.
		addMoves(findStateChanges(statei+1, len(states),
			state, states, begNodesByState, endNodesByState), state)

		// Handle clean additions of state.
		addMoves(StringsIntersectStrings(StringsRemoveStrings(
			endNodesByState[state], begNodesByState[state]),
			adds),
			state)

		// Handle clean removals of state.
		addMoves(StringsIntersectStrings(StringsRemoveStrings(
			begNodesByState[state], endNodesByState[state]),
			subs),
			"")
	}

	return moves
}

func findStateChanges(begStateIdx, endStateIdx int,
	state string, states []string,
	begNodesByState map[string][]string,
	endNodesByState map[string][]string) (rv []string) {
	for _, node := range endNodesByState[state] {
		for i := begStateIdx; i < endStateIdx; i++ {
			for _, n := range begNodesByState[states[i]] {
				if n == node {
					rv = append(rv, node)
				}
			}
		}
	}

	return rv
}
