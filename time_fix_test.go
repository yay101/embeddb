package embeddb

import (
	"testing"
	"time"
)

func TestTimeMmapReload(t *testing.T) {
	dbPath := "test_time_reload.db"
	cleanupTestFiles(dbPath)
	defer cleanupTestFiles(dbPath)

	db, _ := New[TestRecord](dbPath, false, true)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// First insert + early query
	db.Insert(&TestRecord{Name: "First", Age: 25, CreatedAt: baseTime})
	db.Query("CreatedAt", baseTime)

	// Insert 100 more
	for i := 0; i < 100; i++ {
		r := generateTestRecord(i)
		if i%8 == 0 {
			r.CreatedAt = baseTime
		}
		db.Insert(r)
	}

	// Check mmap
	db.mlock.RLock()
	if db.mfile != nil {
		t.Logf("Before reload: mfile.Len() = %d", db.mfile.Len())
	}
	db.mlock.RUnlock()

	// Trigger reload via Get
	rec, err := db.Get(2)
	t.Logf("Get(2): err=%v", err)
	if rec != nil {
		t.Logf("  Name: %s", rec.Name)
	}

	// Check mmap again
	db.mlock.RLock()
	if db.mfile != nil {
		t.Logf("After Get: mfile.Len() = %d", db.mfile.Len())
	}
	db.mlock.RUnlock()

	// Now try Query
	results, _ := db.Query("CreatedAt", baseTime)
	t.Logf("Query returned %d results", len(results))
}
