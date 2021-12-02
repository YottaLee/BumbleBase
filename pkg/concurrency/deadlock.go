package concurrency

import (
	"errors"
	"sync"
)

// Graph.
type Graph struct {
	edges []Edge
	lock  sync.RWMutex
}

// Edge.
type Edge struct {
	from *Transaction
	to   *Transaction
}

// Grab a write lock on the graph
func (g *Graph) WLock() {
	g.lock.Lock()
}

// Release the write lock on the graph
func (g *Graph) WUnlock() {
	g.lock.Unlock()
}

// Grab a read lock on the graph
func (g *Graph) RLock() {
	g.lock.RLock()
}

// Release the write lock on the graph
func (g *Graph) RUnlock() {
	g.lock.RUnlock()
}

// Construct a new graph.
func NewGraph() *Graph {
	return &Graph{edges: make([]Edge, 0)}
}

// Add an edge from `from` to `to`. Logically, `from` waits for `to`.
func (g *Graph) AddEdge(from *Transaction, to *Transaction) {
	g.WLock()
	defer g.WUnlock()
	g.edges = append(g.edges, Edge{from: from, to: to})
}

// Remove an edge. Only removes one of these edges if multiple copies exist.
func (g *Graph) RemoveEdge(from *Transaction, to *Transaction) error {
	g.WLock()
	defer g.WUnlock()
	toRemove := Edge{from: from, to: to}
	for i, e := range g.edges {
		if e == toRemove {
			g.edges = removeEdge(g.edges, i)
			return nil
		}
	}
	return errors.New("edge not found")
}

// Return true if a cycle exists; false otherwise.
func (g *Graph) DetectCycle() bool {
	g.RLock()
	defer g.RUnlock()

	// create a map from transactions to indices
	idxMap := make(map[*Transaction]int, 0)
	for _, e := range g.edges {
		if _, found := idxMap[e.from]; !found {
			idxMap[e.from] = len(idxMap)
		}

		if _, found := idxMap[e.to]; !found {
			idxMap[e.to] = len(idxMap)
		}
	}

	visited := make([]bool, len(idxMap))
	graph := make([][]int, len(idxMap))

	for _, e := range g.edges {
		fromIdx := idxMap[e.from]
		toIdx := idxMap[e.to]

		if graph[fromIdx] == nil {
			graph[fromIdx] = make([]int, 0)
		}

		graph[fromIdx] = append(graph[fromIdx], toIdx)
	}

	for u := 0; u < len(idxMap); u += 1 {
		visited[u] = true
		haveCycle := dfs(graph, visited, u)
		visited[u] = false

		if haveCycle {
			return true
		}
	}

	return false
}

func dfs(graph [][]int, visited []bool, u int) bool {
	if u > len(graph) {
		return false
	}

	for _, v := range graph[u] {
		if visited[v] {
			// A cycle has been detected, return immediately
			return true
		}

		visited[v] = true
		haveCycle := dfs(graph, visited, v)
		visited[v] = false

		if haveCycle {
			return true
		}
	}

	// no cycle detected
	return false
}

// Finds the top-most parent of `t`.
func find(parent []int, t int) int {
	if parent[t] == -1 {
		return t
	}
	return find(parent, parent[t])
}

// Unions the sets that `t1` and `t2` ar ein. Returns true if the two are the same set.
func union(parent []int, t1 int, t2 int) ([]int, bool) {
	p1 := find(parent, t1)
	p2 := find(parent, t2)
	parent[t1] = p2
	return parent, p1 == p2
}

// Gets the index of `t` in the parent array.
func getIndex(transactions []*Transaction, t *Transaction) int {
	for i, x := range transactions {
		if x == t {
			return i
		}
	}
	return -1
}

// Remove the element at index `i` from `l`.
func removeEdge(l []Edge, i int) []Edge {
	l[i] = l[len(l)-1]
	return l[:len(l)-1]
}
