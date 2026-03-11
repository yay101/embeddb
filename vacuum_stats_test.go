package embeddb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func cleanupVacuumStatsFiles(dbPath string) {
	_ = os.Remove(dbPath)
	files, _ := filepath.Glob(dbPath + ".*.idx")
	for _, f := range files {
		_ = os.Remove(f)
	}
}

func TestVacuumFileStats300K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy vacuum stats test")
	}

	const total = 300000
	const deleteEvery = 3
	dbPath := "/tmp/vacuum_stats_300k.db"
	cleanupVacuumStatsFiles(dbPath)
	defer cleanupVacuumStatsFiles(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users, err := db.Table("users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < total; i++ {
		_, err := users.Insert(&PerfUser{
			Name:     fmt.Sprintf("User_%d", i),
			Age:      20 + (i % 80),
			City:     fmt.Sprintf("City_%d", i%20),
			Category: fmt.Sprintf("Cat_%d", i%10),
			Status:   []string{"active", "inactive", "pending"}[i%3],
		})
		if err != nil {
			t.Fatalf("insert failed at %d: %v", i, err)
		}
	}

	deleted := 0
	for id := uint32(1); id <= total; id++ {
		if id%deleteEvery == 0 {
			if err := users.Delete(id); err != nil {
				t.Fatalf("delete failed at id=%d: %v", id, err)
			}
			deleted++
		}
	}

	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	beforeInfo, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	beforeTotal := users.Count()
	beforeActiveRows, err := users.Filter(func(u PerfUser) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	beforeActive := len(beforeActiveRows)

	t.Logf("before vacuum: file=%d bytes total=%d active=%d deleted=%d",
		beforeInfo.Size(), beforeTotal, beforeActive, deleted)

	vacuumStart := time.Now()
	if err := db.Vacuum(); err != nil {
		t.Fatal(err)
	}
	vacuumDur := time.Since(vacuumStart)

	afterInfo, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	afterTotal := users.Count()
	afterActiveRows, err := users.Filter(func(u PerfUser) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	afterActive := len(afterActiveRows)

	reclaimed := beforeInfo.Size() - afterInfo.Size()
	t.Logf("after vacuum: file=%d bytes total=%d active=%d reclaimed=%d bytes vacuum=%s",
		afterInfo.Size(), afterTotal, afterActive, reclaimed, vacuumDur)

	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("expected vacuum to reduce file size: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	if afterTotal != beforeActive {
		t.Fatalf("expected index count after vacuum to equal active before vacuum: got=%d want=%d", afterTotal, beforeActive)
	}

	if afterActive != beforeActive {
		t.Fatalf("active records changed unexpectedly across vacuum: before=%d after=%d", beforeActive, afterActive)
	}
}
