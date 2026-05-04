package embeddb

import (
	"fmt"
	"os"
	"testing"
)

func TestWALCrashRecovery(t *testing.T) {
	dbPath := "/tmp/test_wal_crash.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, err := Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		_, err := tbl.Insert(&TestRecord{
			Name: fmt.Sprintf("before_crash_%d", i),
			Data: "persisted",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Close()

	walExists := false
	if _, err := os.Stat(dbPath + ".wal"); err == nil {
		walExists = true
	}
	if !walExists {
		t.Log("WAL file cleaned up on close (expected with checkpoint)")
	}

	db, err = Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err = Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(1); i <= 50; i++ {
		record, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get record %d after reopen: %v", i, err)
		}
		if record.Name != fmt.Sprintf("before_crash_%d", i-1) {
			t.Errorf("record %d: expected 'before_crash_%d', got '%s'", i, i-1, record.Name)
		}
	}

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
}

func TestWALUncleanShutdown(t *testing.T) {
	dbPath := "/tmp/test_wal_unclean.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, err := Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		_, err := tbl.Insert(&TestRecord{
			Name: fmt.Sprintf("saved_%d", i),
			Data: "data",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Close()

	db, err = Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err = Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 30; i < 60; i++ {
		_, err := tbl.Insert(&TestRecord{
			Name: fmt.Sprintf("unsaved_%d", i),
			Data: "crash data",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	db.database.file.Close()
	db.database.region.Store(nil)
	if db.database.wal != nil {
		db.database.wal.file.Close()
		db.database.wal.file = nil
	}

	walExists := false
	if _, err := os.Stat(dbPath + ".wal"); err == nil {
		walExists = true
	}
	if !walExists {
		t.Fatal("expected WAL file to exist after unclean shutdown")
	}

	db, err = Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err = Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(1); i <= 60; i++ {
		record, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get record %d after crash recovery: %v", i, err)
		}
		if i <= 30 {
			expected := fmt.Sprintf("saved_%d", i-1)
			if record.Name != expected {
				t.Errorf("record %d: expected '%s', got '%s'", i, expected, record.Name)
			}
		} else {
			expected := fmt.Sprintf("unsaved_%d", i-1)
			if record.Name != expected {
				t.Errorf("record %d: expected '%s', got '%s'", i, expected, record.Name)
			}
		}
	}

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
}

func TestWALBasicOperations(t *testing.T) {
	dbPath := "/tmp/test_wal_basic.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, err := Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	id, err := tbl.Insert(&TestRecord{Name: "wal test", Data: "data"})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Name != "wal test" {
		t.Errorf("expected 'wal test', got '%s'", record.Name)
	}

	err = tbl.Update(id, &TestRecord{Name: "wal updated", Data: "new data"})
	if err != nil {
		t.Fatal(err)
	}

	record, err = tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Name != "wal updated" {
		t.Errorf("expected 'wal updated', got '%s'", record.Name)
	}

	err = tbl.Delete(id)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tbl.Get(id)
	if err == nil {
		t.Error("expected error after delete")
	}

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
}

func TestWALRestartPersistence(t *testing.T) {
	dbPath := "/tmp/test_wal_restart.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, err := Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		_, err := tbl.Insert(&TestRecord{
			Name: fmt.Sprintf("item_%d", i),
			Data: "persistent",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Close()

	db, err = Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err = Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(1); i <= 100; i++ {
		record, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get record %d after restart: %v", i, err)
		}
		if record.Name != fmt.Sprintf("item_%d", i-1) {
			t.Errorf("record %d: expected 'item_%d', got '%s'", i, i-1, record.Name)
		}
	}

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
}

func TestWALVacuum(t *testing.T) {
	dbPath := "/tmp/test_wal_vacuum.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, err := Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		_, err := tbl.Insert(&TestRecord{Name: "record", Data: "data"})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := uint32(1); i <= 25; i++ {
		tbl.Delete(i)
	}

	err = db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	for i := uint32(26); i <= 50; i++ {
		record, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get record %d after vacuum: %v", i, err)
		}
		if record.Name != "record" {
			t.Errorf("record %d: expected 'record', got '%s'", i, record.Name)
		}
	}

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
}

func TestWALSync(t *testing.T) {
	dbPath := "/tmp/test_wal_sync.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, err := Open(dbPath, OpenOptions{
		WAL: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		_, err := tbl.Insert(&TestRecord{Name: "sync test", Data: "data"})
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	err = db.FastSync()
	if err != nil {
		t.Fatalf("FastSync failed: %v", err)
	}

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
}
