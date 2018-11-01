package util

import (
	"errors"
	"fmt"
	"math"
)

type edge struct {
	i, j int
}

type edgeSet struct {
	set []edge
}

func (s *edgeSet) pop() (bool, edge) {
	l := len(s.set)
	if l == 0 {
		return false, edge{}
	}
	e := s.set[l-1]
	s.set = s.set[:l-1]
	return true, e
}

func (s *edgeSet) add(e edge) {
	s.set = append(s.set, e)
}

type tree struct {
	n         int
	root      int
	leftPrec  []int
	rightPrec []int
	edges     edgeSet
}

func makeTree(n int, r int, e []edge) tree {
	left := make([]int, n)
	right := make([]int, n)
	for i := 0; i < n; i++ {
		left[i] = -1
		right[i] = -1
	}
	left[r] = r
	return tree{n, r, left, right, edgeSet{e}}
}

// Returns a couple of values:
// - a boolean: whether the extension has been successful
// - the index of the new element in the tree (when successful)
func (t *tree) extend() (bool, int) {
	b, e := t.edges.pop()
	if b {
		t.rightPrec[e.j] = e.i
		return true, e.j
	}
	return false, -1
}

// Returns two arrays of indices. The first contain the indices of the elements
// in the left side of the tree. The second constains the indices of the
// elements in the right side of the tree. All the indices are sorted in
// increasing order.
func (t *tree) indices() ([]int, []int) {
	l := make([]int, 0, t.n)
	r := make([]int, 0, t.n)

	for i := 0; i < t.n; i++ {
		if t.leftPrec[i] != -1 {
			l = append(l, i)
		}
		if t.rightPrec[i] != -1 {
			r = append(r, i)
		}
	}

	return l, r
}

// Add the edge (i, j) to the tree
func (t *tree) addEdge(i int, j int) {
	t.leftPrec[i] = j
}

// Return the path to the root starting from the given vertex.
func (t *tree) pathToRoot(end int) []int {
	path := make([]int, 0, 2*t.n)

	j := end
	i := t.rightPrec[j]

	for i != t.root {
		path = append(path, j, i)
		j = t.leftPrec[i]
		i = t.rightPrec[j]
	}

	path = append(path, j, i)

	return path
}

func (t *tree) addTightEdges(edges []edge) {
	for _, e := range edges {
		t.edges.add(e)
	}
}

type label struct {
	n      int
	costs  [][]int //costs
	left   []int   // labels on the rows
	right  []int   // labels on the columns
	slack  []int   // min slack
	slackI []int   // min slack index
}

func makeLabel(n int, costs [][]int) label {
	left := make([]int, n)
	right := make([]int, n)
	slack := make([]int, n)
	slackI := make([]int, n)
	return label{n, costs, left, right, slack, slackI}
}

// initialize left = min_j cost[i][j] for each row i
func (l *label) initialize() {
	for i := 0; i < l.n; i++ {
		l.left[i] = l.costs[i][0]
		for j := 1; j < l.n; j++ {
			if l.costs[i][j] < l.left[i] {
				l.left[i] = l.costs[i][j]
			}
		}
	}
}

// Returns whether a given edge is tight
func (l *label) isTight(i int, j int) bool {
	return l.costs[i][j]-l.left[i]-l.right[j] == 0
}

// Given a set s of row indices and a set of column indices update the labels.
// Assumes that each indices set is sorted and contains no duplicate.
func (l *label) update(s []int, t []int) []edge {
	// find the minimum slack
	min := -1
	idx := 0
	for j := 0; j < l.n; j++ {
		if idx < len(t) && j == t[idx] {
			idx++
			continue
		}
		sl := l.slack[j]
		if min == -1 || sl < min {
			min = sl
		}
	}

	// increase the label on the elements of s
	for _, i := range s {
		l.left[i] += min
	}
	// decrease the label on the elements of t
	for _, i := range t {
		l.right[i] -= min
	}

	// decrease each slack by min and cache the tight edges
	edges := make([]edge, 0, l.n)
	idx = 0
	for j := 0; j < l.n; j++ {
		if idx < len(t) && j == t[idx] {
			idx++
			continue
		}
		l.slack[j] -= min
		if l.slack[j] == 0 {
			edges = append(edges, edge{l.slackI[j], j})
		}
	}

	return edges
}

func (l *label) initializeSlacks(i int) []edge {
	edges := make([]edge, 0, l.n)
	for j := 0; j < l.n; j++ {
		l.slack[j] = l.costs[i][j] - l.left[i] - l.right[j]
		l.slackI[j] = i
		if l.slack[j] == 0 {
			edges = append(edges, edge{i, j})
		}
	}
	return edges
}

func (l *label) updateSlacks(i int) []edge {
	edges := make([]edge, 0, l.n)
	for j := 0; j < l.n; j++ {
		s := l.costs[i][j] - l.left[i] - l.right[j]
		if s < l.slack[j] {
			l.slack[j] = s
			l.slackI[j] = i
			if l.slack[j] == 0 {
				edges = append(edges, edge{i, j})
			}
		}
	}
	return edges
}

type matching struct {
	n  int
	ij []int
	ji []int
}

// Returns an empty matching of the given size
func makeMatching(n int) matching {
	ij := make([]int, n)
	ji := make([]int, n)
	for i := 0; i < n; i++ {
		ij[i] = -1
		ji[i] = -1
	}
	return matching{n, ij, ji}
}

// Greedily build a matching
func (m *matching) initialize(isTight func(int, int) bool) {
	for i := 0; i < m.n; i++ {
		for j := 0; j < m.n; j++ {
			if isTight(i, j) && (m.ji[j] == -1) {
				m.ij[i] = j
				m.ji[j] = i
				break
			}
		}
	}
}

// Returns whether the matching is perfect and a free vertex (when the matching is not perfect)
func (m *matching) isPerfect() (bool, int) {
	for i := 0; i < m.n; i++ {
		if m.ij[i] == -1 {
			return false, i
		}
	}
	return true, -1
}

// Returns whether the vertex of the right set is matched, when true is also
// returns the match
func (m *matching) isMatched(j int) (bool, int) {
	i := m.ji[j]
	return (i != -1), i
}

// Returns an array `a` representing the edges of the matching in the form `(i, a[i])`.
func (m *matching) format() []int {
	return m.ij
}

// Augments the matching using the given augmenting path.
func (m *matching) augment(p []int) {
	for idx, j := range p {
		if idx%2 == 0 {
			i := p[idx+1]
			m.ij[i] = j
			m.ji[j] = i
		}
	}
}

func fillZeros(costs [][]int) ([][]int, int) {
	n := len(costs)
	if n == 0 {
		return nil, 0
	}
	m := len(costs[0])
	minDim := int(math.Min(float64(m), float64(n)))
	maxDim := int(math.Max(float64(m), float64(n)))
	newCosts := make([][]int, maxDim)
	for i := 0; i < n; i++ {
		newCosts[i] = make([]int, maxDim)
		for j := 0; j < m; j++ {
			newCosts[i][j] = costs[i][j]
		}
	}
	return newCosts, minDim
}

func validate(costs [][]int) error {
	n := len(costs)

	if n == 0 {
		return errors.New("The costs matrix is empty.")
	}

	if m := len(costs[0]); m != n {
		return errors.New("The costs matrix is not square.")
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if costs[i][j] < 0 {
				return fmt.Errorf("The coefficient (%d,%d) is negative.", i, j)
			}
		}
	}

	return nil
}

func Solve(costs [][]int) ([]int, error) {
	costs, minDim := fillZeros(costs)
	// Validate the input
	if err := validate(costs); err != nil {
		return []int{}, err
	}

	n := len(costs)
	label := makeLabel(n, costs) // labels on the row and columns
	match := makeMatching(n)     // matching using tight edges

	label.initialize()
	match.initialize(label.isTight)

	// loop until the matching is perfect
	for p, r := match.isPerfect(); !p; p, r = match.isPerfect() {
		e := label.initializeSlacks(r) // initializes the min slacks to r
		t := makeTree(n, r, e)         // alternating tree rooted at r

		// loop until the matching is augmented
		for true {
			var j int // new column index in the tree

			// Extend the tree
			if b, k := t.extend(); b {
				j = k
			} else {
				u, v := t.indices()
				e := label.update(u, v)
				t.addTightEdges(e)
				_, j = t.extend()
			}

			if b, i := match.isMatched(j); b {
				// Add the edge (i, j) to the tree
				t.addEdge(i, j)
				e := label.updateSlacks(i)
				t.addTightEdges(e)
			} else {
				// Augment the matching
				path := t.pathToRoot(j)
				match.augment(path)
				break
			}
		}
	}
	sol := refine(match.ij, minDim)
	return sol, nil
	// return match.format(), nil
}

func refine(sol []int, nJobs int) []int {
	n := len(sol)
	if n == 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		if sol[i] >= nJobs {
			sol[i] = int(-1)
		}
	}
	return sol
}

func main() {
	// a := [][]int{{5, 4, 9999, 9999, 9999, 9999}, {11, 10, 9999, 9999, 9999, 9999}, {13, 12, 9999, 9999, 9999, 9999}, {6, 5, 9999, 9999, 9999, 9999}, {21, 20, 9999, 9999, 9999, 9999}, {23, 22, 9999, 9999, 9999, 9999}}
	// a := [][]int{{5, 4, 0, 0, 0, 0}, {11, 10, 0, 0, 0, 0}, {13, 12, 0, 0, 0, 0}, {6, 5, 0, 0, 0, 0}, {21, 20, 0, 0, 0, 0}, {23, 22, 0, 0, 0, 0}}
	// a := [][]int{{5, 11, 13, 6, 21, 23}, {4, 10, 12, 12, 5, 20}}
	a := [][]int{{5, 4}, {11, 10}, {13, 12}, {6, 3}, {21, 20}, {23, 22}}
	sol, _ := Solve(a)
	// sol = refine(sol, 2)
	fmt.Println(sol)
}
