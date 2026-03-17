package embeddb

import (
	"path/filepath"
	"testing"
)

type NoIdxRecord struct {
	ID   uint32 `db:"id,primary"`
	Name string `db:"index"`
	Age  int    `db:"index"`
}

func assertNoIndexFiles(t *testing.T, dbPath string) {
	t.Helper()

	patterns := []string{dbPath + ".*.idx", dbPath + ".idx"}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatalf("glob failed for %s: %v", pattern, err)
		}
		if len(matches) > 0 {
			t.Fatalf("expected no runtime .idx files, found: %v", matches)
		}
	}
}

func TestNoRuntimeIdxFilesCreated(t *testing.T) {
	t.Setenv("EMBEDDB_EXPERIMENTAL_REGION_INDEX", "1")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "no_idx.db")

	db, err := New[NoIdxRecord](dbPath, false, true)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	for i := 0; i < 200; i++ {
		_, err := db.Insert(&NoIdxRecord{Name: "u", Age: 20 + (i % 5)})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if _, err := db.Query("Age", 22); err != nil {
		t.Fatalf("query age: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	assertNoIndexFiles(t, dbPath)

	db2, err := New[NoIdxRecord](dbPath, false, true)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()

	res, err := db2.Query("Age", 22)
	if err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	if len(res) == 0 {
		t.Fatalf("expected results after reopen")
	}

	assertNoIndexFiles(t, dbPath)
}
