package segtree

import (
	"fmt"
)

// Main interface to access the segment tree
type Tree interface {
	Push(xmin, ymin, xmax, ymax int) // Push new interval to tree

	Count(x, y float64) int // Query the count of intervals that covers the given point

	Query(x, y float64) []*Interval // Query intervals that covers the given point

	Print() // Print the tree structure to stdout

	ToString() string // plain string represents segments

	SegmentArray() []*SquareSegment // An array of pointers to segment in bfs iterator sequence
}

type SquareSegment struct {
	xmin int
	xmax int
	ymin int
	ymax int
}

const (
	// Relationships of two segments
	EQUAL = iota
	SUBSET
	INTERSECT_OR_SUPERSET
	DISJOINT
)

func (s *SquareSegment) CompareTo(other *SquareSegment) int {
	if other.ymin >= s.ymax || other.ymax <= s.ymin ||
		other.xmin >= s.xmax || other.xmax <= s.xmin {
		return DISJOINT
	}
	if other.xmin == s.xmin && other.xmax == s.xmax &&
		other.ymin == s.ymin && other.ymax == s.ymax {
		return EQUAL
	}
	if other.xmin <= s.xmin && other.xmax >= s.xmax &&
		other.ymin <= s.ymin && other.ymax >= s.ymax {
		return SUBSET
	}
	return INTERSECT_OR_SUPERSET
}

// return true if the segment covers the point
func (s *SquareSegment) CoverPoint(x, y float64) bool {
	if float64(s.xmin) <= x && float64(s.xmax) >= x &&
		float64(s.ymin) <= y && float64(s.ymax) >= y {
		return true
	}
	return false
}

func (s *SquareSegment) ToString() string {
	return fmt.Sprintf("(%d %d %d %d)", s.xmin, s.xmax, s.ymin, s.ymax)
}

// Interval represents elements pushed into stree
type Interval struct {
	Id int // unique
	SquareSegment
}

func (ival *Interval) SegmentEqual(other *Interval) bool {
	return ival.SquareSegment.CompareTo(&other.SquareSegment) == EQUAL
}

func (ival *Interval) Print() {
	fmt.Printf("(%d: %d %d %d %d)",
		ival.Id,
		ival.SquareSegment.xmin, ival.SquareSegment.xmax,
		ival.SquareSegment.ymin, ival.SquareSegment.ymax)
}

func (ival *Interval) ToString() string {
	return fmt.Sprintf("%d:%s", ival.Id, ival.SquareSegment.ToString())
}

type node struct {
	level int // level in segment tree

	segment SquareSegment

	parent *node // parent node

	upleft, upright, lowleft, lowright *node // four child nodes

	overlap []*Interval // All intervals that overlap with segment
}

func NewNode(seg SquareSegment, level int, parent *node) *node {
	n := &node{segment: seg, level: level, parent: parent}
	n.upleft, n.upright, n.lowleft, n.lowright = nil, nil, nil, nil
	n.overlap = make([]*Interval, 0)
	return n
}

func (n *node) ChildrenNotNil() []*node {
	nodes := make([]*node, 0)
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
	var termGreen string = "\x1b[32;1m"
	var termNoColor string = "\x1b[0m"
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
	count int // Number of segments

	root *node
	base []*Interval // Interval stack

	xmin int // Min value of the first dimension of all intervals
	xmax int // Max value of the first dimension of all intervals
	ymin int // Min value of the second dimension of all intervals
	ymax int // Max value of the second dimension of all intervals
}

func (t *stree) Push(xmin, xmax, ymin, ymax int) {
	new_interval := &Interval{t.count,
		SquareSegment{xmin: xmin, xmax: xmax, ymin: ymin, ymax: ymax}}
	t.count++

	t.base = append(t.base, new_interval)
	insertInterval(t.root, new_interval)
}

func (t *stree) Count(x, y float64) int {
	return countSingle(t.root, x, y)
}

func countSingle(node *node, x, y float64) int {
	count := 0
	if node.segment.CoverPoint(x, y) {
		count = len(node.overlap)
		for _, child := range node.ChildrenNotNil() {
			count += countSingle(child, x, y)
		}
	}
	return count
}

func (t *stree) Query(x, y float64) []*Interval {
	result := make(map[int]*Interval)
	querySingle(t.root, x, y, &result)

	sl := make([]*Interval, 0, len(result))
	for _, interval := range result {
		sl = append(sl, interval)
	}
	return sl
}

func querySingle(node *node, x, y float64, result *map[int]*Interval) {
	if node.segment.CoverPoint(x, y) {
		for _, interval := range node.overlap {
			(*result)[(*interval).Id] = interval
		}
		for _, child := range node.ChildrenNotNil() {
			querySingle(child, x, y, result)
		}
	}
}

func (t *stree) SegmentArray() []*SquareSegment {
	segments := make([]*SquareSegment, 0)

	nstack := make([]*node, 0)
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
	nstack := make([]*node, 0)
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
			level += 1
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
	var n *node = NewNode(segment, level, parent)

	level += 1
	if xsep == xmax && ysep == ymax {
		// Leaf node, do nothine
	} else if xsep == xmax {
		if (ymax - ymin) > 2 {
			n.lowleft = insertNodes(xmin, xmax, ymin, ysep, level, n)
			n.upleft = insertNodes(xmin, xmax, ysep, ymax, level, n)
		} else {
			lowleft_seg := SquareSegment{xmin: xmin, xmax: xmax, ymin: ymin, ymax: ysep}
			upleft_seg := SquareSegment{xmin: xmin, xmax: xmax, ymin: ysep, ymax: ymax}
			n.lowleft = NewNode(lowleft_seg, level, n)
			n.upleft = NewNode(upleft_seg, level, n)
		}
	} else if ysep == ymax {
		if (xmax - xmin) > 2 {
			n.lowleft = insertNodes(xmin, xsep, ymin, ymax, level, n)
			n.lowright = insertNodes(xsep, xmax, ymin, ymax, level, n)
		} else {
			lowleft_seg := SquareSegment{xmin: xmin, xmax: xsep, ymin: ymin, ymax: ymax}
			lowright_seg := SquareSegment{xmin: xsep, xmax: xmax, ymin: ymin, ymax: ymax}
			n.lowleft = NewNode(lowleft_seg, level, n)
			n.lowright = NewNode(lowright_seg, level, n)
		}
	} else {
		n.lowleft = insertNodes(xmin, xsep, ymin, ysep, level, n)
		n.lowright = insertNodes(xsep, xmax, ymin, ysep, level, n)
		n.upright = insertNodes(xsep, xmax, ysep, ymax, level, n)
		n.upleft = insertNodes(xmin, xsep, ysep, ymax, level, n)
	}
	return n
}

func NewTree(xmin, xmax, ymin, ymax int) Tree {
	t := new(stree)

	t.count = 0
	t.base = make([]*Interval, 0)

	t.xmin = xmin
	t.xmax = xmax
	t.ymin = ymin
	t.ymax = ymax

	t.root = insertNodes(xmin, xmax, ymin, ymax, 0, nil)

	return t
}

func insertInterval(node *node, interval *Interval) {
	switch node.segment.CompareTo(&interval.SquareSegment) {
	case EQUAL, SUBSET:
		node.overlap = append(node.overlap, interval)
	case INTERSECT_OR_SUPERSET:
		for _, child := range node.ChildrenNotNil() {
			insertInterval(child, interval)
		}
	case DISJOINT:
		// do nothing
	}
}
