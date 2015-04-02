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

func clearMap(m *map[int]*Interval) {
	for k, _ := range *m {
		delete(*m, k)
	}
}

func TestPushAndQuery(t *testing.T) {
	tree := NewTree(0, 4, 0, 4)
	tree.Push(0, 1, 0, 1)
	tree.Push(1, 2, 1, 2)
	tree.Push(1, 3, 1, 3)
	tree.Push(0, 2, 0, 2)
	tree.Push(2, 4, 2, 4)
	tree.Push(2, 4, 1, 4)
	tree.Push(0, 3, 0, 2)
	tree.Print()

	var intervals []*Interval

	intervals = tree.Query(0.5, 0.5)
	expected := make(map[int]*Interval, 0)
	expected[0] = &Interval{0, SquareSegment{0, 1, 0, 1}}
	expected[3] = &Interval{3, SquareSegment{0, 2, 0, 2}}
	expected[6] = &Interval{6, SquareSegment{0, 3, 0, 2}}
	if len(intervals) != len(expected) {
		t.Errorf("error query result for (0.5, 0.5)")
	}
	if count := tree.Count(0.5, 0.5); count != len(expected) {
		t.Errorf("error query count result %d for (0.5, 0.5), expected %d", count, len(expected))
	}
	for _, interval := range intervals {
		if expect, ok := expected[interval.Id]; !ok {
			t.Errorf("error query result %s, not expected", interval.ToString())
		} else if !interval.SegmentEqual(expect) {
			t.Errorf("error query result %s, wrong segment", interval.ToString())
		}
	}

	clearMap(&expected)
	intervals = tree.Query(1, 1)
	expected[0] = &Interval{0, SquareSegment{0, 1, 0, 1}}
	expected[1] = &Interval{1, SquareSegment{1, 2, 1, 2}}
	expected[2] = &Interval{2, SquareSegment{1, 3, 1, 3}}
	expected[3] = &Interval{0, SquareSegment{0, 2, 0, 2}}
	expected[6] = &Interval{6, SquareSegment{0, 3, 0, 2}}
	if len(intervals) != len(expected) {
		t.Errorf("error query result for (1, 1)")
	}
	if count := tree.Count(1, 1); count != len(expected) {
		t.Errorf("error query count result %d for (1, 1), expected %d", count, len(expected))
	}
	for _, interval := range intervals {
		if expect, ok := expected[interval.Id]; !ok {
			t.Errorf("error query result %s, not expected", interval.ToString())
		} else if !interval.SegmentEqual(expect) {
			t.Errorf("error query result %s, wrong segment", interval.ToString())
		}
	}

	clearMap(&expected)
	intervals = tree.Query(2.5, 2.5)
	expected[2] = &Interval{2, SquareSegment{1, 3, 1, 3}}
	expected[4] = &Interval{4, SquareSegment{2, 4, 2, 4}}
	expected[5] = &Interval{5, SquareSegment{2, 4, 1, 4}}
	if len(intervals) != len(expected) {
		t.Errorf("error query result for (2.5, 2.5)")
	}
	if count := tree.Count(2.5, 2.5); count != len(expected) {
		t.Errorf("error query count result %d for (2.5, 2.5), expected %d", count, len(expected))
	}
	for _, interval := range intervals {
		if expect, ok := expected[interval.Id]; !ok {
			t.Errorf("error query result %s, not expected", interval.ToString())
		} else if !interval.SegmentEqual(expect) {
			t.Errorf("error query result %s, wrong segment", interval.ToString())
		}
	}

	clearMap(&expected)
	intervals = tree.Query(5, 5)
	if len(intervals) != len(expected) {
		t.Errorf("error query result for (5, 5)")
	}
	if count := tree.Count(5, 5); count != len(expected) {
		t.Errorf("error query count result %d for (5, 5), expected %d", count, len(expected))
	}
}
