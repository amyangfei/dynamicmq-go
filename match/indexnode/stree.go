package main

import (
	"fmt"
	"sync"
)

// Tree is the main interface to access the segment tree
type Tree interface {
	Push(xmin, xmax, ymin, ymax int, data *[]byte) *Interval // Push new interval to tree

	Delete(interval *Interval) // Delete specific Interval from tree

	Count() int // Return interval count

	QueryCount(x, y float64) int // Query the count of intervals that covers the given point

	Query(x, y float64) []*Interval // Query intervals that covers the given point

	Print() // Print the tree structure to stdout

	ToString() string // plain string represents segments

	SegmentArray() []*SquareSegment // An array of pointers to segment in bfs iterator sequence
}

// SquareSegment struct
type SquareSegment struct {
	xmin int
	xmax int
	ymin int
	ymax int
}

// Relationships of two segments
const (
	// Equal means the two segments have the same boundary
	Equal = iota
	// Subset means the first segment is fully covered the second one but not
	// equal
	Subset
	// IntersectOrSuperset means the first segment intersects with the second
	// one or it is the subset of the second
	IntersectOrSuperset
	// Disjoint means the two segments have no intersect part
	Disjoint
)

// CompareTo returns the relationship between this SquareSegment and the other
func (s *SquareSegment) CompareTo(other *SquareSegment) int {
	if other.ymin >= s.ymax || other.ymax <= s.ymin ||
		other.xmin >= s.xmax || other.xmax <= s.xmin {
		return Disjoint
	}
	if other.xmin == s.xmin && other.xmax == s.xmax &&
		other.ymin == s.ymin && other.ymax == s.ymax {
		return Equal
	}
	if other.xmin <= s.xmin && other.xmax >= s.xmax &&
		other.ymin <= s.ymin && other.ymax >= s.ymax {
		return Subset
	}
	return IntersectOrSuperset
}

// coverPoint returns true if the segment covers the point
func (s *SquareSegment) coverPoint(x, y float64) bool {
	if float64(s.xmin) <= x && float64(s.xmax) >= x &&
		float64(s.ymin) <= y && float64(s.ymax) >= y {
		return true
	}
	return false
}

// ToString returns a format string of this SquareSegment
func (s *SquareSegment) ToString() string {
	return fmt.Sprintf("(%d %d %d %d)", s.xmin, s.xmax, s.ymin, s.ymax)
}

// Interval represents elements pushed into stree
type Interval struct {
	ID   int64   // unique
	Data *[]byte // stores pointer to subclient id, unique
	SquareSegment
}

// SegmentEqual detects if this interval has the same range on each dimension
// to a given interval other.
func (ival *Interval) SegmentEqual(other *Interval) bool {
	return ival.SquareSegment.CompareTo(&other.SquareSegment) == Equal
}

// Print prints the interval in specific format
func (ival *Interval) Print() {
	fmt.Printf("(%d: %d %d %d %d)",
		ival.ID,
		ival.SquareSegment.xmin, ival.SquareSegment.xmax,
		ival.SquareSegment.ymin, ival.SquareSegment.ymax)
}

// ToString returns a format string for the interval
func (ival *Interval) ToString() string {
	return fmt.Sprintf("%d:%s", ival.ID, ival.SquareSegment.ToString())
}

type node struct {
	level int // level in segment tree

	segment SquareSegment

	parent *node // parent node

	upleft, upright, lowleft, lowright *node // four child nodes

	overlap []*Interval // All intervals that overlap with segment
}

func newNode(seg SquareSegment, level int, parent *node) *node {
	n := &node{segment: seg, level: level, parent: parent}
	n.upleft, n.upright, n.lowleft, n.lowright = nil, nil, nil, nil
	n.overlap = make([]*Interval, 0)
	return n
}

func (n *node) ChildrenNotNil() []*node {
	var nodes []*node
	if n.lowleft != nil {
		nodes = append(nodes, n.lowleft)
	}
	if n.lowright != nil {
		nodes = append(nodes, n.lowright)
	}
	if n.upright != nil {
		nodes = append(nodes, n.upright)
	}
	if n.upleft != nil {
		nodes = append(nodes, n.upleft)
	}
	return nodes
}

func (n *node) Print() {
	termGreen := "\x1b[32;1m"
	termNoColor := "\x1b[0m"
	// print segment
	if n.IsLeaf() {
		fmt.Printf("%s(%d %d %d %d)%s",
			termGreen,
			n.segment.xmin, n.segment.xmax, n.segment.ymin, n.segment.ymax,
			termNoColor)
	} else {
		fmt.Printf("(%d %d %d %d)",
			n.segment.xmin, n.segment.xmax, n.segment.ymin, n.segment.ymax)
	}
	fmt.Printf("{")
	for _, interval := range n.overlap {
		interval.Print()
	}
	fmt.Printf("}")
}

func (n *node) IsLeaf() bool {
	return (n.segment.ymax-n.segment.ymin == 1) && (n.segment.xmax-n.segment.xmin == 1)
}

type stree struct {
	lock  *sync.RWMutex
	root  *node
	base  map[int64]*Interval // mapping from Interval Id to Interval
	maxid int64               // next available Interval id

	xmin int // Min value of the first dimension of all intervals
	xmax int // Max value of the first dimension of all intervals
	ymin int // Min value of the second dimension of all intervals
	ymax int // Max value of the second dimension of all intervals
}

func (t *stree) Push(xmin, xmax, ymin, ymax int, data *[]byte) *Interval {
	t.lock.Lock()
	defer t.lock.Unlock()
	newInterval := &Interval{
		ID:            t.maxid,
		Data:          data,
		SquareSegment: SquareSegment{xmin: xmin, xmax: xmax, ymin: ymin, ymax: ymax},
	}

	t.maxid++
	t.base[newInterval.ID] = newInterval
	insertInterval(t.root, newInterval)

	return newInterval
}

func (t *stree) Delete(interval *Interval) {
	t.lock.Lock()
	defer t.lock.Unlock()
	// remove interval from all the segments containing it.
	deleteInterval(t.root, interval)

	// remove interval from tree.base
	delete(t.base, interval.ID)
}

func deleteInterval(node *node, interval *Interval) {
	switch node.segment.CompareTo(&interval.SquareSegment) {
	case Equal, Subset:
		// TODO: Time Complexity O(n), should be better
		var idx int
		for idx = 0; idx < len(node.overlap); idx++ {
			if node.overlap[idx] == interval {
				break
			}
		}
		node.overlap[len(node.overlap)-1], node.overlap =
			nil, append(node.overlap[:idx], node.overlap[idx+1:]...)
	case IntersectOrSuperset:
		for _, child := range node.ChildrenNotNil() {
			deleteInterval(child, interval)
		}
	case Disjoint:
		// do nothing
	}
}

func (t *stree) Count() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return len(t.base)
}

func (t *stree) QueryCount(x, y float64) int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return countSingle(t.root, x, y)
}

func countSingle(node *node, x, y float64) int {
	count := 0
	if node.segment.coverPoint(x, y) {
		count = len(node.overlap)
		for _, child := range node.ChildrenNotNil() {
			count += countSingle(child, x, y)
		}
	}
	return count
}

func (t *stree) Query(x, y float64) []*Interval {
	t.lock.RLock()
	defer t.lock.RUnlock()
	result := make(map[int64]*Interval)
	querySingle(t.root, x, y, &result)

	sl := make([]*Interval, 0, len(result))
	for _, interval := range result {
		sl = append(sl, interval)
	}
	return sl
}

func querySingle(node *node, x, y float64, result *map[int64]*Interval) {
	if node.segment.coverPoint(x, y) {
		for _, interval := range node.overlap {
			(*result)[(*interval).ID] = interval
		}
		for _, child := range node.ChildrenNotNil() {
			querySingle(child, x, y, result)
		}
	}
}

func (t *stree) SegmentArray() []*SquareSegment {
	var segments []*SquareSegment

	var nstack []*node
	nstack = append(nstack, t.root)
	for len(nstack) > 0 {
		n := nstack[0]
		nstack = nstack[1:]

		segments = append(segments, &n.segment)

		for _, child := range n.ChildrenNotNil() {
			nstack = append(nstack, child)
		}
	}

	return segments
}

func (t *stree) ToString() string {
	s := ""
	segments := t.SegmentArray()
	for _, seg := range segments {
		s += fmt.Sprintf("%v", *seg)
	}
	return s
}

func (t *stree) Print() {
	var nstack []*node
	nstack = append(nstack, t.root)
	level := 0
	var lastparent *node
	for len(nstack) > 0 {
		n := nstack[0]
		nstack = nstack[1:]

		// print seprator between nodes who have different parent node.
		if lastparent != n.parent {
			lastparent = n.parent
			fmt.Printf(" | ")
		}

		// print seprator for a new tree level
		if n.level > level {
			fmt.Println()
			level++
		}

		n.Print()

		for _, child := range n.ChildrenNotNil() {
			nstack = append(nstack, child)
		}
	}
	fmt.Println("\n---------")
}

func insertNodes(xmin, xmax, ymin, ymax, level int, parent *node) *node {
	xsep := (xmin + xmax + 1) / 2
	ysep := (ymin + ymax + 1) / 2

	segment := SquareSegment{xmin: xmin, xmax: xmax, ymin: ymin, ymax: ymax}
	n := newNode(segment, level, parent)

	level++
	if xsep == xmax && ysep == ymax {
		// Leaf node, do nothine
	} else if xsep == xmax {
		if (ymax - ymin) > 2 {
			n.lowleft = insertNodes(xmin, xmax, ymin, ysep, level, n)
			n.upleft = insertNodes(xmin, xmax, ysep, ymax, level, n)
		} else {
			lowleftSeg := SquareSegment{xmin: xmin, xmax: xmax, ymin: ymin, ymax: ysep}
			upleftSeg := SquareSegment{xmin: xmin, xmax: xmax, ymin: ysep, ymax: ymax}
			n.lowleft = newNode(lowleftSeg, level, n)
			n.upleft = newNode(upleftSeg, level, n)
		}
	} else if ysep == ymax {
		if (xmax - xmin) > 2 {
			n.lowleft = insertNodes(xmin, xsep, ymin, ymax, level, n)
			n.lowright = insertNodes(xsep, xmax, ymin, ymax, level, n)
		} else {
			lowleftSeg := SquareSegment{xmin: xmin, xmax: xsep, ymin: ymin, ymax: ymax}
			lowrightSeg := SquareSegment{xmin: xsep, xmax: xmax, ymin: ymin, ymax: ymax}
			n.lowleft = newNode(lowleftSeg, level, n)
			n.lowright = newNode(lowrightSeg, level, n)
		}
	} else {
		n.lowleft = insertNodes(xmin, xsep, ymin, ysep, level, n)
		n.lowright = insertNodes(xsep, xmax, ymin, ysep, level, n)
		n.upright = insertNodes(xsep, xmax, ysep, ymax, level, n)
		n.upleft = insertNodes(xmin, xsep, ysep, ymax, level, n)
	}
	return n
}

// NewTree creates a stree struct which implements the Tree interface
func NewTree(xmin, xmax, ymin, ymax int) Tree {
	t := &stree{
		lock:  new(sync.RWMutex),
		maxid: 0,
		base:  make(map[int64]*Interval),
		xmin:  xmin,
		xmax:  xmax,
		ymin:  ymin,
		ymax:  ymax,
	}
	t.root = insertNodes(xmin, xmax, ymin, ymax, 0, nil)

	return t
}

func insertInterval(node *node, interval *Interval) {
	switch node.segment.CompareTo(&interval.SquareSegment) {
	case Equal, Subset:
		node.overlap = append(node.overlap, interval)
	case IntersectOrSuperset:
		for _, child := range node.ChildrenNotNil() {
			insertInterval(child, interval)
		}
	case Disjoint:
		// do nothing
	}
}
