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

type NodeState struct {
	Node  string
	State string
}

// The states should be ordered by most superior state first.  The
// begNodesByState and endNodesByState are keyed is stateName, and
// value is an array of node names.  For example, {"master": ["a"],
// "replica": ["b", "c"]}.
func CalcPartitionMoves(
	states []string,
	begNodesByState map[string][]string,
	endNodesByState map[string][]string,
) []NodeState {
	/*
	   	var moves []NodeState

	   	nodesAll := map[string]bool{}
	   	for _, nodes := range begNodesByState {
	   		for _, node := range nodes {
	   			if !nodesAll[node] {
	   				nodesAll[node] = true
	   			}
	   		}
	   	}
	   	for _, nodes := range endNodesByState {
	   		for _, node := range nodes {
	   			if !nodesAll[node] {
	   				nodesAll[node] = true
	   			}
	   		}
	   	}

	   	for statei, state := range states {
	           // Handle demotions of superiorTo(state) to state.
	   		for j := statei - 1; j >= 0; j-- {
	   			// superiorState := states[j]
	   			// begNodesByState[state]
	   		}
	   		// Handle promotions of inferiorTo(state) to state.
	   		// Handle clean additions of state.
	   		// Handle clean removals of state.
	   	}
	*/

	return nil
}
