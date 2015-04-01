package segtree

import (
	"fmt"
	"testing"
)

func TreeEqual(tree Tree, segs []*SquareSegment) bool {
	segments := tree.SegmentArray()
	if len(segments) != len(segs) {
		return false
	}
	for i := 0; i < len(segments); i++ {
		if (*segments[i]).CompareTo(segs[i]) != EQUAL {
			return false
		}
	}
	return true
}

func TestNewTree(t *testing.T) {
	for i := 1; i < 6; i++ {
		tree := NewTree(0, i, 0, i)
		fmt.Printf("square=%d\n", i)
		tree.Print()
	}

	tree := NewTree(0, 1, 0, 1)
	segs := []*SquareSegment{&SquareSegment{0, 1, 0, 1}}
	if !TreeEqual(tree, segs) {
		t.Errorf("wrong segments array: %s", tree.ToString())
	}

	tree = NewTree(0, 1, 0, 2)
	segs = []*SquareSegment{
		&SquareSegment{0, 1, 0, 2},
		&SquareSegment{0, 1, 0, 1}, &SquareSegment{0, 1, 1, 2},
	}
	if !TreeEqual(tree, segs) {
		t.Errorf("wrong segments array: %s", tree.ToString())
	}

	tree = NewTree(0, 2, 0, 1)
	segs = []*SquareSegment{
		&SquareSegment{0, 2, 0, 1},
		&SquareSegment{0, 1, 0, 1}, &SquareSegment{1, 2, 0, 1},
	}
	if !TreeEqual(tree, segs) {
		t.Errorf("wrong segments array: %s", tree.ToString())
	}

	tree = NewTree(0, 3, 0, 3)
	segs = []*SquareSegment{
		&SquareSegment{0, 3, 0, 3},

		&SquareSegment{0, 2, 0, 2}, &SquareSegment{2, 3, 0, 2},
		&SquareSegment{2, 3, 2, 3}, &SquareSegment{0, 2, 2, 3},

		&SquareSegment{0, 1, 0, 1}, &SquareSegment{1, 2, 0, 1},
		&SquareSegment{1, 2, 1, 2}, &SquareSegment{0, 1, 1, 2},
		&SquareSegment{2, 3, 0, 1}, &SquareSegment{2, 3, 1, 2},
		&SquareSegment{0, 1, 2, 3}, &SquareSegment{1, 2, 2, 3},
	}
	if !TreeEqual(tree, segs) {
		t.Errorf("wrong segments array: %s", tree.ToString())
	}

	tree = NewTree(0, 4, 0, 4)
	segs = []*SquareSegment{
		&SquareSegment{0, 4, 0, 4},

		&SquareSegment{0, 2, 0, 2}, &SquareSegment{2, 4, 0, 2},
		&SquareSegment{2, 4, 2, 4}, &SquareSegment{0, 2, 2, 4},

		&SquareSegment{0, 1, 0, 1}, &SquareSegment{1, 2, 0, 1},
		&SquareSegment{1, 2, 1, 2}, &SquareSegment{0, 1, 1, 2},
		&SquareSegment{2, 3, 0, 1}, &SquareSegment{3, 4, 0, 1},
		&SquareSegment{3, 4, 1, 2}, &SquareSegment{2, 3, 1, 2},
		&SquareSegment{2, 3, 2, 3}, &SquareSegment{3, 4, 2, 3},
		&SquareSegment{3, 4, 3, 4}, &SquareSegment{2, 3, 3, 4},
		&SquareSegment{0, 1, 2, 3}, &SquareSegment{1, 2, 2, 3},
		&SquareSegment{1, 2, 3, 4}, &SquareSegment{0, 1, 3, 4},
	}
	if !TreeEqual(tree, segs) {
		t.Errorf("wrong segments array: %s", tree.ToString())
	}

}
