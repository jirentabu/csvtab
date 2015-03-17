// table单元测试文件
// tabu 2014-08-11
package csvtab

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func fillTable() *Table {
	r := rand.New(rand.NewSource(100))
	table := NewTable()
	table.AddColumns(3)
	for i := 0; i < 10; i++ {
		row := make([]string, 3)
		row[0] = fmt.Sprint(r.Float32())
		row[1] = fmt.Sprint(r.Float32())
		row[2] = fmt.Sprint(r.Float32())
		table.Append(row)
	}
	return table
}

func TestReadAll(t *testing.T) {
	file, err := os.Open("/Users/tabu/test/csv.txt")
	if err != nil {
		fmt.Println(err.Error())
		t.Fatal(err)
	}
	defer file.Close()

	table, err := ReadAll(csv.NewReader(file), true)
	if err != nil {
		t.Fatal(err)
	}

	if table.GetCount() == 0 {
		t.Fatal("table is empty.")
	}
}

func TestOrderBy(t *testing.T) {
	table := fillTable()
	table.OrderBy("Column1")
	for i := 0; i < table.GetCount()-1; i++ {
		a := table.Get(i, "Column1")
		b := table.Get(i+1, "Column1")
		if a > b {
			t.Error(a, b)
		}
	}

	table.OrderBy("Column2", LessFloatAES)
	for i := 0; i < table.GetCount()-1; i++ {
		a := table.Get(i, "Column2")
		b := table.Get(i+1, "Column2")
		if !LessFloatAES(a, b) {
			t.Error(a, b)
		}
	}

	// display
	for i := 0; i < table.GetCount(); i++ {
		fmt.Println(table.Get(i, "Column1"), table.Get(i, "Column2"), table.Get(i, "Column3"))
	}
}

func TestQuery(t *testing.T) {
	table := NewTable()
	table.AddColumns(3)
	table.Append([]string{"1", "1", "1"})
	table.Append([]string{"1", "2", "1"})
	table.Append([]string{"2", "1", "1"})
	table.Append([]string{"2", "2", "1"})
	table.Append([]string{"3", "3", "1"})

	table.CreateHashIndex("Column1")
	hash := table.getHash([]string{"Column1"})

	if hash == nil {
		t.Error("create index error.")
	}

	q := table.Query(KV{"Column1", "2"})
	for i := 0; i < q.GetCount(); i++ {
		if q.Get(i, "Column1") != "2" {
			t.Error(q.Get(i, "Column1"))
		}
	}
	fmt.Println("query result:")
	for i := 0; i < q.GetCount(); i++ {
		fmt.Println(q.Get(i, "Column1"), q.Get(i, "Column2"), q.Get(i, "Column3"))
	}

	if q.GetCount() != 2 {
		t.Error(q.GetCount())
	}
}
