package embeddb

import (
	"os"
	"sync"
	"testing"
)

type SyncTestRecord struct {
	ID   uint32 `db:"id,primary"`
	Name string
}

func TestFastSync(t *testing.T) {
	os.Remove("/tmp/test_fastsync.db")
	defer os.Remove("/tmp/test_fastsync.db")

	db, err := Open("/tmp/test_fastsync.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SyncTestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&SyncTestRecord{Name: "before-sync"})

	err = db.FastSync()
	if err != nil {
		t.Fatalf("FastSync failed: %v", err)
	}

	tbl.Insert(&SyncTestRecord{Name: "after-sync"})

	if count := tbl.Count(); count != 2 {
		t.Errorf("expected 2 records, got %d", count)
	}
}

func TestFastSyncOnClosed(t *testing.T) {
	os.Remove("/tmp/test_fastsync_closed.db")
	defer os.Remove("/tmp/test_fastsync_closed.db")

	db, err := Open("/tmp/test_fastsync_closed.db")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	err = db.FastSync()
	if err == nil {
		t.Error("expected error from FastSync on closed DB")
	}
}

func TestConcurrentVacuumWithReads(t *testing.T) {
	os.Remove("/tmp/test_concurrent_vacuum.db")
	defer os.Remove("/tmp/test_concurrent_vacuum.db")

	db, err := Open("/tmp/test_concurrent_vacuum.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SyncTestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&SyncTestRecord{Name: "record"})
	}

	for i := uint32(1); i <= 50; i++ {
		tbl.Delete(i)
	}

	var wg sync.WaitGroup
	var readErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, err := tbl.All()
			if err != nil && readErr == nil {
				readErr = err
			}
		}
	}()

	wg.Wait()

	if readErr != nil {
		t.Errorf("concurrent read error: %v", readErr)
	}

	err = db.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	err = db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}

	if count := tbl.Count(); count != 50 {
		t.Errorf("expected 50 records after vacuum, got %d", count)
	}
}

func TestBTreeDeleteBasic(t *testing.T) {
	os.Remove("/tmp/test_btree_delete.db")
	defer os.Remove("/tmp/test_btree_delete.db")

	db, err := Open("/tmp/test_btree_delete.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SyncTestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		tbl.Insert(&SyncTestRecord{Name: "record"})
	}

	all, err := tbl.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 20 {
		t.Fatalf("expected 20 records, got %d", len(all))
	}

	for i := uint32(1); i <= 10; i++ {
		err := tbl.Delete(i)
		if err != nil {
			t.Fatalf("failed to delete record %d: %v", i, err)
		}
	}

	if count := tbl.Count(); count != 10 {
		t.Errorf("expected 10 records after delete, got %d", count)
	}

	remaining, err := tbl.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 10 {
		t.Errorf("expected 10 remaining, got %d", len(remaining))
	}

	for i := uint32(11); i <= 20; i++ {
		_, err := tbl.Get(i)
		if err != nil {
			t.Errorf("expected record %d to exist after deleting 1-10: %v", i, err)
		}
	}
}

func TestBTreeDeleteAllThenInsert(t *testing.T) {
	os.Remove("/tmp/test_btree_delete_all.db")
	defer os.Remove("/tmp/test_btree_delete_all.db")

	db, err := Open("/tmp/test_btree_delete_all.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SyncTestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		tbl.Insert(&SyncTestRecord{Name: "first"})
	}

	for i := uint32(1); i <= 10; i++ {
		tbl.Delete(i)
	}

	if count := tbl.Count(); count != 0 {
		t.Errorf("expected 0 records after deleting all, got %d", count)
	}

	for i := 0; i < 10; i++ {
		tbl.Insert(&SyncTestRecord{Name: "second"})
	}

	if count := tbl.Count(); count != 10 {
		t.Errorf("expected 10 records after re-insert, got %d", count)
	}

	all, err := tbl.All()
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range all {
		if rec.Name != "second" {
			t.Errorf("expected all records to have Name='second', got %q", rec.Name)
		}
	}
}

func TestInsertManyError(t *testing.T) {
	os.Remove("/tmp/test_insertmany_err.db")
	defer os.Remove("/tmp/test_insertmany_err.db")

	db, err := Open("/tmp/test_insertmany_err.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SyncTestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	records := []*SyncTestRecord{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Gamma"},
	}

	ids, err := tbl.InsertMany(records)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("expected 3 IDs, got %d", len(ids))
	}
	if tbl.Count() != 3 {
		t.Errorf("expected 3 records, got %d", tbl.Count())
	}
}

func TestUpdateManyError(t *testing.T) {
	os.Remove("/tmp/test_updatemany_err.db")
	defer os.Remove("/tmp/test_updatemany_err.db")

	db, err := Open("/tmp/test_updatemany_err.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SyncTestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&SyncTestRecord{Name: "Alpha"})
	tbl.Insert(&SyncTestRecord{Name: "Beta"})

	updated, err := tbl.UpdateMany([]any{uint32(1), uint32(2)}, func(r *SyncTestRecord) error {
		r.Name = "Updated-" + r.Name
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if updated != 2 {
		t.Errorf("expected 2 updated, got %d", updated)
	}

	rec, _ := tbl.Get(uint32(1))
	if rec.Name != "Updated-Alpha" {
		t.Errorf("expected 'Updated-Alpha', got %q", rec.Name)
	}
}

func TestDropCleansVersionIndex(t *testing.T) {
	os.Remove("/tmp/test_drop_version.db")
	defer os.Remove("/tmp/test_drop_version.db")

	db, err := Open("/tmp/test_drop_version.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[VersionedDoc](db, "docs", UseOptions{MaxVersions: 3})
	if err != nil {
		t.Fatal(err)
	}

	id, _ := tbl.Insert(&VersionedDoc{Title: "v1", Content: "c1", Version: 1})
	tbl.Update(id, &VersionedDoc{ID: id, Title: "v2", Content: "c2", Version: 2})

	versions, err := tbl.ListVersions(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions before drop, got %d", len(versions))
	}

	err = tbl.Drop()
	if err != nil {
		t.Fatalf("Drop failed: %v", err)
	}

	tbl2, err := Use[VersionedDoc](db, "docs", UseOptions{MaxVersions: 3})
	if err != nil {
		t.Fatal(err)
	}

	id2, _ := tbl2.Insert(&VersionedDoc{Title: "fresh", Content: "c", Version: 1})

	versions2, err := tbl2.ListVersions(id2)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions2) != 1 {
		t.Errorf("expected 1 version for fresh record after drop, got %d", len(versions2))
	}
}
