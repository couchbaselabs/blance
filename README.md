blance
======

blance implements a straightforward partition assignment algorithm,
using a greedy, heuristic, functional approach.

[![GoDoc](https://godoc.org/github.com/couchbaselabs/blance?status.svg)](https://godoc.org/github.com/couchbaselabs/blance)

LICENSE: Apache 2.0

### Usage

See the PlanNextMap() function as a starting point.

### For developers

To get local coverage reports with heatmaps...

    go test -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out
