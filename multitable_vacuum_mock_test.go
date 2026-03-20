package embeddb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type mockNestedLoc struct {
	City string
	Zone int
}

type mockMultiRow struct {
	Name   string
	Group  string
	Active bool
	Loc    mockNestedLoc
}

func logMockMem(t *testing.T, stage string) {
	t.Helper()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("%s heap=%dKB alloc=%dMB sys=%dMB gc=%d", stage, ms.HeapAlloc/1024, ms.TotalAlloc/1024/1024, ms.Sys/1024/1024, ms.NumGC)
}

func removeMockDBFiles(t *testing.T, dbPath string) {
	t.Helper()
	_ = os.Remove(dbPath)
	files, err := filepath.Glob(dbPath + ".*.idx")
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	for _, f := range files {
		_ = os.Remove(f)
	}
}

func insertMockRows(t *testing.T, table *Table[mockMultiRow], tableTag string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := table.Insert(&mockMultiRow{
			Name:   fmt.Sprintf("%s_name_%d", tableTag, i),
			Group:  fmt.Sprintf("G%d", i%8),
			Active: i%2 == 0,
			Loc: mockNestedLoc{
				City: fmt.Sprintf("City_%d", i%5),
				Zone: i % 11,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func expectNestedCityCount(t *testing.T, table *Table[mockMultiRow], city string, want int) {
	t.Helper()
	rows, err := table.Filter(func(r mockMultiRow) bool { return r.Loc.City == city })
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != want {
		t.Fatalf("nested field query/filter %q expected %d got %d", city, want, len(rows))
	}
}

func expectNestedCityQueryPaged(t *testing.T, table *Table[mockMultiRow], city string, pageSize, wantTotal int) {
	t.Helper()

	p1, err := table.QueryPaged("Loc.City", city, 0, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if p1.TotalCount != wantTotal {
		t.Fatalf("query paged total expected %d got %d", wantTotal, p1.TotalCount)
	}
	if len(p1.Records) != pageSize {
		t.Fatalf("query paged page1 expected %d got %d", pageSize, len(p1.Records))
	}
	if !p1.HasMore {
		t.Fatal("query paged page1 expected HasMore=true")
	}

	p2, err := table.QueryPaged("Loc.City", city, pageSize, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Records) != pageSize {
		t.Fatalf("query paged page2 expected %d got %d", pageSize, len(p2.Records))
	}
}

func expectNestedCityFilterPaged(t *testing.T, table *Table[mockMultiRow], city string, pageSize, wantTotal int) {
	t.Helper()

	p1, err := table.FilterPaged(func(r mockMultiRow) bool { return r.Loc.City == city }, 0, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if p1.TotalCount != wantTotal {
		t.Fatalf("filter paged total expected %d got %d", wantTotal, p1.TotalCount)
	}
	if len(p1.Records) != pageSize {
		t.Fatalf("filter paged page1 expected %d got %d", pageSize, len(p1.Records))
	}
	if !p1.HasMore {
		t.Fatal("filter paged page1 expected HasMore=true")
	}

	p2, err := table.FilterPaged(func(r mockMultiRow) bool { return r.Loc.City == city }, pageSize, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Records) != pageSize {
		t.Fatalf("filter paged page2 expected %d got %d", pageSize, len(p2.Records))
	}
}

func TestMockMultiTableVacuumReopenNested100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy 100k-per-table mock test")
	}

	const n = 100000
	dbPath := "/tmp/mock_multitable_vacuum.db"
	removeMockDBFiles(t, dbPath)
	defer removeMockDBFiles(t, dbPath)

	logMockMem(t, "start")

	db, err := New[mockMultiRow](dbPath, false, false)
	if err != nil {
		t.Fatal(err)
	}

	t1, err := db.Table("table_one")
	if err != nil {
		t.Fatal(err)
	}
	t2, err := db.Table("table_two")
	if err != nil {
		t.Fatal(err)
	}
	t3, err := db.Table("table_three")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tableIDs initial: t1=%d t2=%d t3=%d", t1.tableID, t2.tableID, t3.tableID)

	if err := t1.CreateIndex("Loc.City"); err != nil {
		t.Fatal(err)
	}

	insertMockRows(t, t1, "t1", n)
	logMockMem(t, "after table_one")

	insertMockRows(t, t2, "t2", n)
	logMockMem(t, "after table_two")

	insertMockRows(t, t3, "t3", n)
	logMockMem(t, "after table_three")
	t.Logf("counts before first close: t1=%d t2=%d t3=%d", t1.Count(), t2.Count(), t3.Count())

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen before reads so mmap reflects all appended data.
	dbRead, err := New[mockMultiRow](dbPath, false, false)
	if err != nil {
		t.Fatal(err)
	}
	t1r0, err := dbRead.Table("table_one")
	if err != nil {
		t.Fatal(err)
	}
	t2r0, err := dbRead.Table("table_two")
	if err != nil {
		t.Fatal(err)
	}
	t3r0, err := dbRead.Table("table_three")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tableIDs reopen: t1=%d t2=%d t3=%d", t1r0.tableID, t2r0.tableID, t3r0.tableID)
	t.Logf("counts before vacuum: t1=%d t2=%d t3=%d", t1r0.Count(), t2r0.Count(), t3r0.Count())
	if err := t1r0.CreateIndex("Loc.City"); err != nil {
		t.Fatal(err)
	}
	expectNestedCityCount(t, t1r0, "City_2", n/5)
	expectNestedCityCount(t, t2r0, "City_3", n/5)
	expectNestedCityCount(t, t3r0, "City_1", n/5)
	expectNestedCityQueryPaged(t, t1r0, "City_2", 250, n/5)
	expectNestedCityFilterPaged(t, t3r0, "City_1", 250, n/5)

	// Drop one table, vacuum, then close.
	if err := t2r0.Drop(); err != nil {
		t.Fatal(err)
	}
	if err := dbRead.Vacuum(); err != nil {
		t.Fatal(err)
	}
	if err := dbRead.Close(); err != nil {
		t.Fatal(err)
	}

	logMockMem(t, "after drop+vacuum")

	// Remove hidden index cache to exercise reopen path with missing index artifacts.
	cacheDir := filepath.Join(filepath.Dir(dbPath), ".embeddb-indexes")
	_ = os.RemoveAll(cacheDir)

	// Reopen and re-query remaining tables on nested fields.
	db2, err := New[mockMultiRow](dbPath, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	t1r, err := db2.Table("table_one")
	if err != nil {
		t.Fatal(err)
	}
	t3r, err := db2.Table("table_three")
	if err != nil {
		t.Fatal(err)
	}

	expectNestedCityCount(t, t1r, "City_2", n/5)
	expectNestedCityCount(t, t3r, "City_1", n/5)
	expectNestedCityFilterPaged(t, t1r, "City_2", 250, n/5)
	expectNestedCityFilterPaged(t, t3r, "City_1", 250, n/5)

	t2r, err := db2.Table("table_two")
	if err != nil {
		t.Fatal(err)
	}
	if t2r.Count() != 0 {
		t.Fatalf("expected dropped table_two to be empty after vacuum, got %d", t2r.Count())
	}

	logMockMem(t, "end")
}
