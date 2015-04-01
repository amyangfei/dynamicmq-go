package segtree

import (
	"fmt"
)

// Main interface to access the segment tree
type Tree interface {
	Push(xmin, ymin, xmax, ymax int) // Push new interval to tree

	Query(x, y int) // Query interval that covers the given point

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
	SUPERSET
	INTERSECT
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
	if other.xmin >= s.xmin && other.xmax <= s.xmax &&
		other.ymin >= s.ymin && other.ymax <= s.ymax {
		return SUPERSET
	}
	return INTERSECT
}

// Interval represents elements pushed into stree
type Interval struct {
	Id int // unique
	SquareSegment
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
	if n.IsLeaf() {
		fmt.Printf("%s(%d %d %d %d)%s",
			termGreen,
			n.segment.xmin, n.segment.xmax, n.segment.ymin, n.segment.ymax,
			termNoColor)
	} else {
		fmt.Printf("(%d %d %d %d)",
			n.segment.xmin, n.segment.xmax, n.segment.ymin, n.segment.ymax)
	}
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

func (t *stree) Push(x1, y1, x2, y2 int) {
}

func (t *stree) Query(x, y int) {
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
