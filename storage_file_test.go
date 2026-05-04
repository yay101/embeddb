package embeddb

import (
	"os"
	"testing"
)

func TestStorageFileBasicWriteRead(t *testing.T) {
	os.Remove("/tmp/test_filemode_basic.db")

	db, err := Open("/tmp/test_filemode_basic.db", OpenOptions{
		StorageMode: StorageFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	id, err := tbl.Insert(&TestRecord{Name: "hello world", Data: "file mode"})
	if err != nil {
		t.Fatal(err)
	}

	if id != 1 {
		t.Errorf("expected id 1, got %d", id)
	}

	record, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if record.Name != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", record.Name)
	}
	if record.Data != "file mode" {
		t.Errorf("expected 'file mode', got '%s'", record.Data)
	}

	db.Close()
	os.Remove("/tmp/test_filemode_basic.db")
}

func TestStorageFileMultipleRecords(t *testing.T) {
	os.Remove("/tmp/test_filemode_multi.db")

	db, err := Open("/tmp/test_filemode_multi.db", OpenOptions{
		StorageMode: StorageFile,
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
			Name: "record",
			Data: "data",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := uint32(1); i <= 100; i++ {
		record, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get record %d: %v", i, err)
		}
		if record.Name != "record" {
			t.Errorf("record %d: expected 'record', got '%s'", i, record.Name)
		}
	}

	db.Close()
	os.Remove("/tmp/test_filemode_multi.db")
}

func TestStorageFileUpdateDelete(t *testing.T) {
	os.Remove("/tmp/test_filemode_update.db")

	db, err := Open("/tmp/test_filemode_update.db", OpenOptions{
		StorageMode: StorageFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	id, err := tbl.Insert(&TestRecord{Name: "original"})
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Update(id, &TestRecord{Name: "updated"})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Name != "updated" {
		t.Errorf("expected 'updated', got '%s'", record.Name)
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
	os.Remove("/tmp/test_filemode_update.db")
}

func TestStorageFileRestart(t *testing.T) {
	os.Remove("/tmp/test_filemode_restart.db")

	db, err := Open("/tmp/test_filemode_restart.db", OpenOptions{
		StorageMode: StorageFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		_, err := tbl.Insert(&TestRecord{Name: "persist"})
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Close()

	db, err = Open("/tmp/test_filemode_restart.db", OpenOptions{
		StorageMode: StorageFile,
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
			t.Fatalf("failed to get record %d after restart: %v", i, err)
		}
		if record.Name != "persist" {
			t.Errorf("record %d: expected 'persist', got '%s'", i, record.Name)
		}
	}

	db.Close()
	os.Remove("/tmp/test_filemode_restart.db")
}

func TestStorageFileVacuum(t *testing.T) {
	os.Remove("/tmp/test_filemode_vacuum.db")

	db, err := Open("/tmp/test_filemode_vacuum.db", OpenOptions{
		StorageMode: StorageFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		_, err := tbl.Insert(&TestRecord{Name: "record"})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := uint32(1); i <= 10; i++ {
		tbl.Delete(i)
	}

	err = db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	for i := uint32(11); i <= 20; i++ {
		record, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get record %d after vacuum: %v", i, err)
		}
		if record.Name != "record" {
			t.Errorf("record %d: expected 'record', got '%s'", i, record.Name)
		}
	}

	db.Close()
	os.Remove("/tmp/test_filemode_vacuum.db")
}

func TestStorageFileSync(t *testing.T) {
	os.Remove("/tmp/test_filemode_sync.db")

	db, err := Open("/tmp/test_filemode_sync.db", OpenOptions{
		StorageMode: StorageFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = tbl.Insert(&TestRecord{Name: "sync test"})
	if err != nil {
		t.Fatal(err)
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
	os.Remove("/tmp/test_filemode_sync.db")
}
