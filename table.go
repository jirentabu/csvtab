//
// csv 内存表
// tabu 2014-08-05
package csvtab

import (
	"encoding/csv"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const (
	OrderAES = 1
	OrderDES = -1
)

type Table struct {
	Columns  []string
	ColIndex map[string]int
	Rows     [][]string
	Indexs   []*HashIndex
}

// by name
type KV struct {
	Name, Value string
}

type HashIndex struct {
	Columns []string
	Index   map[string][]int
}

func NewTable() *Table {
	return &Table{
		[]string{}, map[string]int{}, [][]string{}, nil,
	}
}

func ReadAll(r *csv.Reader, header bool) (*Table, error) {
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	table := NewTable()
	if len(records) == 0 {
		return table, nil
	}

	if header {
		table.SetColumns(records[0])
		table.Rows = records[1:]
	} else {
		table.AddColumns(len(records[0]))
		table.Rows = records
	}
	return table, nil
}

func (t *Table) AddColumns(count int) {
	columns := make([]string, count)
	for i := 0; i < count; i++ {
		columns[i] = fmt.Sprintf("Column%d", i+1)
	}
	t.SetColumns(columns)
}

func (t *Table) SetColumns(columns []string) {
	idx := make(map[string]int, len(columns))
	for i, v := range columns {
		idx[strings.Trim(v, " ,")] = i
	}
	t.Columns = columns
	t.ColIndex = idx
}

func (t *Table) RowCount() int {
	return len(t.Rows)
}

func (t *Table) ColumnCount() int {
	return len(t.Columns)
}

func (t *Table) GetCount() int {
	return t.RowCount()
}

func (t *Table) Get(i int, column string) string {
	if j, ok := t.MapOf(column); ok {
		return t.Rows[i][j]
	}

	panic(column)
}

func (t *Table) MapOf(column string) (index int, ok bool) {
	index, ok = t.ColIndex[column]
	return
}

func (t *Table) Map(names []string) ([]int, bool) {
	intCols := make([]int, len(names))
	for i, v := range names {
		if index, ok := t.MapOf(v); ok {
			intCols[i] = index
		} else {
			return nil, false
		}
	}
	return intCols, true
}

func (t *Table) Append(row []string) {
	t.Rows = append(t.Rows, row)
}

type Predicate func(row []string) bool

func (t *Table) FindAll(start int, f Predicate) *Table {
	table := Table{
		Columns:  t.Columns,
		ColIndex: t.ColIndex,
		Rows:     make([][]string, 0),
	}

	for _, r := range t.Rows {
		if f(r) {
			table.Append(r)
		}
	}
	return &table
}

func (t *Table) FindFirst(start int, f Predicate) int {
	for i := start; i < len(t.Rows); i++ {
		if f(t.Rows[i]) {
			return i
		}
	}
	return -1
}

// column1,lessfunc1, column2, lessfunc2
func (t *Table) OrderBy(columnLessFunc ...interface{}) *Table {
	less := []ColumnLess{}
	for _, v := range columnLessFunc {
		valueType := reflect.TypeOf(v)
		if valueType.Kind() == reflect.String {
			name := v.(string)
			if idx, ok := t.MapOf(name); ok {
				less = append(less, ColumnLess{idx, LessStringAES})
			} else {
				panic("invalid column name " + name)
			}
		} else if valueType.Kind() == reflect.Func {
			less[len(less)-1].Less = v.(func(string, string) bool)
		}
	}

	if len(less) > 0 {
		OrderedBy(less).Sort(t.Rows)
	}
	return t
}

func (t *Table) Distinct(cols ...string) *Table {
	intCols, ok := t.Map(cols)
	if ok {
		return t.DistinctI(intCols)
	} else {
		return t
	}
}

func (t *Table) DistinctI(cols []int) *Table {
	table := NewTable()
	columns := make([]string, 0, len(cols))
	for _, c := range cols {
		columns = append(columns, t.Columns[c])
	}
	table.SetColumns(columns)

	dist := make(map[string]int)
	for _, r := range t.Rows {
		key := t.getKey(r, cols)
		if _, ok := dist[key]; !ok {
			dist[key] = len(table.Rows)
			row := make([]string, len(table.Columns))
			for i, c := range cols {
				row[i] = r[c]
			}
			table.Append(row)
		}
	}
	return table
}

// hash index
func (t *Table) CreateHashIndex(cols ...string) *HashIndex {
	intCols, ok := t.Map(cols)
	if !ok {
		return nil
	}

	if hash := t.getHash(cols); hash != nil {
		return hash
	}

	// create new hash
	hash := &HashIndex{cols, make(map[string][]int)}
	for i, r := range t.Rows {
		key := t.getKey(r, intCols)
		if hash.Index[key] == nil {
			hash.Index[key] = make([]int, 0)
		}
		hash.Index[key] = append(hash.Index[key], i)
	}

	// add index
	if t.Indexs == nil {
		t.Indexs = make([]*HashIndex, 0, 3)
	}
	t.Indexs = append(t.Indexs, hash)
	return hash
}

func (t *Table) Query(values ...KV) *Table {
	cols := make([]string, len(values))
	for i, v := range values {
		cols[i] = v.Name
	}

	// 如果找到对应的hash索引按hash查找，否则全表搜索
	hash := t.getHash(cols)
	if hash != nil {
		// fmt.Println("query for hash index.")
		key := ""
		for _, v := range values {
			key += v.Value
		}
		table := NewTable()
		table.Columns = t.Columns
		table.ColIndex = t.ColIndex

		if rows, ok := hash.Index[key]; ok {
			table.Rows = make([][]string, 0, len(rows))
			for _, i := range rows {
				table.Rows = append(table.Rows, t.Rows[i])
			}
		}
		return table
	} else {
		// fmt.Println("query for full table.")
		intCols, ok := t.Map(cols)
		if !ok {
			panic(values)
		}

		f := func(row []string) bool {
			for i, c := range intCols {
				if row[c] != values[i].Value {
					return false
				}
			}
			return true
		}
		return t.FindAll(0, f)
	}
}

func (t *Table) Search(orderedColumnValue ...KV) int {
	cols := make([]string, len(orderedColumnValue))
	for _, v := range orderedColumnValue {
		cols = append(cols, v.Name)
	}
	intCols, ok := t.Map(cols)
	if !ok {
		panic(orderedColumnValue)
	}

	f := func(k int) bool {
		r := t.Rows[k]
		for i, v := range intCols {
			if r[v] < orderedColumnValue[i].Value {
				return true
			} else if r[v] > orderedColumnValue[i].Value {
				return false
			}
		}
		return false
	}
	return sort.Search(len(t.Rows), f)
}

func (t *Table) getHash(cols []string) *HashIndex {
	for _, index := range t.Indexs {
		if len(index.Columns) != len(cols) {
			continue
		}

		eq := true
		for i := 0; i < len(cols); i++ {
			if index.Columns[i] != cols[i] {
				eq = false
				break
			}
		}
		if eq {
			return index
		}
	}
	return nil
}

func (t *Table) getKey(r []string, cols []int) string {
	key := ""
	for _, c := range cols {
		key += r[c]
	}
	return key
}
