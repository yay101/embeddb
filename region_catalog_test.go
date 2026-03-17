package embeddb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type RegionRecord struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Age   int    `db:"index"`
	Score int64
}

func readRegionHeaderPointers(t *testing.T, path string) (start, capacity, used, flags uint32, fileSize int64) {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open db file: %v", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat db file: %v", err)
	}
	fileSize = info.Size()

	h := make([]byte, headerSize)
	if _, err := f.ReadAt(h, 0); err != nil {
		t.Fatalf("read header: %v", err)
	}

	start = binary.BigEndian.Uint32(h[43:47])
	capacity = binary.BigEndian.Uint32(h[47:51])
	used = binary.BigEndian.Uint32(h[51:55])
	flags = binary.BigEndian.Uint32(h[55:59])

	return
}

func TestRegionHeaderPointersPersistAcrossReopen(t *testing.T) {
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX", "1")
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX_UNSAFE", "")

	dir := t.TempDir()
	path := filepath.Join(dir, "region_reopen.db")

	db, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	for i := 0; i < 1500; i++ {
		r := &RegionRecord{Name: fmt.Sprintf("user_%d", i), Age: 20 + (i % 50), Score: int64(i)}
		if _, err := db.Insert(r); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if _, err := db.Query("Name", "user_42"); err != nil {
		t.Fatalf("query name: %v", err)
	}
	if _, err := db.Query("Age", 25); err != nil {
		t.Fatalf("query age: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	start1, cap1, used1, flags1, size1 := readRegionHeaderPointers(t, path)
	if start1 == 0 || cap1 == 0 || used1 == 0 {
		t.Fatalf("expected non-zero region header pointers, got start=%d cap=%d used=%d", start1, cap1, used1)
	}
	if int64(start1)+int64(cap1) > size1 {
		t.Fatalf("region out of bounds after close: start=%d cap=%d size=%d", start1, cap1, size1)
	}
	if used1 > cap1 {
		t.Fatalf("region used > capacity: used=%d cap=%d", used1, cap1)
	}
	if flags1 == 0 {
		t.Fatalf("expected non-zero region flags")
	}

	db2, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}

	res, err := db2.Query("Name", "user_42")
	if err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	if len(res) == 0 {
		t.Fatalf("expected results after reopen")
	}

	if err := db2.Close(); err != nil {
		t.Fatalf("close reopened db: %v", err)
	}

	start2, cap2, used2, _, size2 := readRegionHeaderPointers(t, path)
	if start2 == 0 || cap2 == 0 || used2 == 0 {
		t.Fatalf("expected non-zero region header pointers after reopen")
	}
	if int64(start2)+int64(cap2) > size2 {
		t.Fatalf("region out of bounds after reopen close: start=%d cap=%d size=%d", start2, cap2, size2)
	}
}

func TestRegionHeaderPointersGrowSafely(t *testing.T) {
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX", "1")
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX_UNSAFE", "")

	dir := t.TempDir()
	path := filepath.Join(dir, "region_growth.db")

	db, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	for i := 0; i < 2500; i++ {
		r := &RegionRecord{Name: fmt.Sprintf("seed_user_%d", i), Age: 18 + (i % 60), Score: int64(i)}
		if _, err := db.Insert(r); err != nil {
			t.Fatalf("insert seed %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}

	start1, cap1, used1, _, _ := readRegionHeaderPointers(t, path)
	if cap1 == 0 || used1 == 0 {
		t.Fatalf("expected initial non-zero region pointers")
	}

	db2, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}

	for i := 2500; i < 12000; i++ {
		r := &RegionRecord{Name: fmt.Sprintf("growth_user_%d", i), Age: 18 + (i % 60), Score: int64(i)}
		if _, err := db2.Insert(r); err != nil {
			t.Fatalf("insert growth %d: %v", i, err)
		}
	}

	if _, err := db2.Query("Name", "growth_user_9999"); err != nil {
		t.Fatalf("query after growth: %v", err)
	}

	if err := db2.Close(); err != nil {
		t.Fatalf("close growth db: %v", err)
	}

	start2, cap2, used2, flags2, size2 := readRegionHeaderPointers(t, path)
	if start2 == 0 || cap2 == 0 || used2 == 0 {
		t.Fatalf("expected non-zero region pointers after growth")
	}
	if used2 > cap2 {
		t.Fatalf("region used > capacity after growth: used=%d cap=%d", used2, cap2)
	}
	if int64(start2)+int64(cap2) > size2 {
		t.Fatalf("region out of bounds after growth: start=%d cap=%d size=%d", start2, cap2, size2)
	}
	if flags2 == 0 {
		t.Fatalf("expected non-zero region flags after growth")
	}

	if cap2 < cap1 {
		t.Fatalf("expected capacity to stay same or grow: before=%d after=%d", cap1, cap2)
	}
	if cap2 > cap1 && start2 == start1 {
		// This can still happen if grown in place; this assertion is informational-only.
		t.Logf("capacity grew in place: start=%d cap %d->%d", start2, cap1, cap2)
	}
}

func TestRegionCatalogRecoversFromCorruptFooter(t *testing.T) {
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX", "1")
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX_UNSAFE", "")

	dir := t.TempDir()
	path := filepath.Join(dir, "region_corrupt_footer.db")

	db, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	for i := 0; i < 800; i++ {
		r := &RegionRecord{Name: fmt.Sprintf("user_%d", i), Age: 20 + (i % 30), Score: int64(i)}
		if _, err := db.Insert(r); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	start, cap, _, _, _ := readRegionHeaderPointers(t, path)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open db file for corruption: %v", err)
	}

	footerOffset := int64(start) + int64(cap)
	if _, err := f.WriteAt([]byte("BADFOOT!"), footerOffset); err != nil {
		f.Close()
		t.Fatalf("corrupt footer: %v", err)
	}
	f.Close()

	db2, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("reopen db with corrupt footer: %v", err)
	}

	res, err := db2.Query("Name", "user_42")
	if err != nil {
		t.Fatalf("query after corrupt footer reopen: %v", err)
	}
	if len(res) == 0 {
		t.Fatalf("expected query results after corrupt footer recovery")
	}

	if err := db2.Close(); err != nil {
		t.Fatalf("close recovered db: %v", err)
	}
}

func TestRegionCatalogRebuildsHeaderPointersFromFooter(t *testing.T) {
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX", "1")
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX_UNSAFE", "")

	dir := t.TempDir()
	path := filepath.Join(dir, "region_header_repair.db")

	db, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	for i := 0; i < 1200; i++ {
		r := &RegionRecord{Name: fmt.Sprintf("repair_%d", i), Age: 30 + (i % 20), Score: int64(i)}
		if _, err := db.Insert(r); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open db for header corruption: %v", err)
	}

	h := make([]byte, headerSize)
	if _, err := f.ReadAt(h, 0); err != nil {
		f.Close()
		t.Fatalf("read header: %v", err)
	}

	for i := 43; i < 59; i++ {
		h[i] = 0
	}
	if _, err := f.WriteAt(h, 0); err != nil {
		f.Close()
		t.Fatalf("write corrupt header: %v", err)
	}
	f.Close()

	db2, err := New[RegionRecord](path, false, true)
	if err != nil {
		t.Fatalf("reopen db with zeroed pointers: %v", err)
	}

	res, err := db2.Query("Age", 35)
	if err != nil {
		t.Fatalf("query after header-pointer recovery: %v", err)
	}
	if len(res) == 0 {
		t.Fatalf("expected results after header-pointer recovery")
	}

	if err := db2.Close(); err != nil {
		t.Fatalf("close recovered db: %v", err)
	}

	start, cap, used, flags, _ := readRegionHeaderPointers(t, path)
	if start == 0 || cap == 0 || used == 0 || flags == 0 {
		t.Fatalf("expected repaired non-zero header pointers, got start=%d cap=%d used=%d flags=%d", start, cap, used, flags)
	}
}
