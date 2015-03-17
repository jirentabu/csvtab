package csvtab

import (
	"sort"
	"strconv"
)

type LessFunc func(p1, p2 string) bool

func LessStringAES(p1, p2 string) bool { return p1 < p2 }

func LessStringDES(p1, p2 string) bool { return p1 > p2 }

func LessFloatAES(p1, p2 string) bool {
	f1, err := strconv.ParseFloat(p1, 10)
	if err != nil {
		panic(err)
	}
	f2, err := strconv.ParseFloat(p2, 10)
	if err != nil {
		panic(err)
	}
	return f1 < f2
}

func LessFloatDES(p1, p2 string) bool {
	return !LessFloatAES(p1, p2)
}

type ColumnLess struct {
	Col  int
	Less LessFunc
}

type sorter struct {
	rows [][]string
	cols []ColumnLess
}

func (s *sorter) Sort(rows [][]string) {
	s.rows = rows
	sort.Sort(s)
}

func OrderedBy(less []ColumnLess) *sorter {
	return &sorter{
		cols: less,
	}
}

func (ms *sorter) Len() int {
	return len(ms.rows)
}

func (ms *sorter) Swap(i, j int) {
	ms.rows[i], ms.rows[j] = ms.rows[j], ms.rows[i]
}

func (ms *sorter) Less(i, j int) bool {
	p, q := ms.rows[i], ms.rows[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.cols)-1; k++ {
		c, less := ms.cols[k].Col, ms.cols[k].Less
		switch {
		case less(p[c], q[c]):
			return true
		case less(q[c], p[c]):
			return false
		}
		// p == q; try the next comparison.
	}

	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	c, less := ms.cols[k].Col, ms.cols[k].Less
	return less(p[c], q[c])
}
