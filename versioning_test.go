package embeddb

import (
	"os"
	"testing"
	"time"
)

type VersionedDoc struct {
	ID      uint32 `db:"id,primary"`
	Title   string
	Content string
	Version int
}

func TestVersioningDisabled(t *testing.T) {
	file := "test_versioning_disabled.db"
	defer os.Remove(file)

	db, err := Open(file)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	docs, err := Use[VersionedDoc](db)
	if err != nil {
		t.Fatalf("failedto use table: %v", err)
	}

	id, err := docs.Insert(&VersionedDoc{Title: "v1", Content: "content1", Version: 1})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	err = docs.Update(id, &VersionedDoc{Title: "v2", Content: "content2", Version: 2})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	_, err = docs.GetVersion(id, 1)
	if err == nil {
		t.Error("expected error when versioning is disabled")
	}

	_, err = docs.ListVersions(id)
	if err == nil {
		t.Error("expected error when versioning is disabled")
	}

	doc, err := docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if doc.Version != 2 {
		t.Errorf("expected version 2, got %d", doc.Version)
	}
}

func TestVersioningEnabled(t *testing.T) {
	file := "test_versioning_enabled.db"
	defer os.Remove(file)

	db, err := Open(file)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	docs, err := Use[VersionedDoc](db, UseOptions{MaxVersions: 3})
	if err != nil {
		t.Fatalf("failed to use table: %v", err)
	}

	id, err := docs.Insert(&VersionedDoc{Title: "v1", Content: "content1", Version: 1})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	err = docs.Update(id, &VersionedDoc{Title: "v2", Content: "content2", Version: 2})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	err = docs.Update(id, &VersionedDoc{Title: "v3", Content: "content3", Version: 3})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	versions, err := docs.ListVersions(id)
	if err != nil {
		t.Fatalf("failed to list versions: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected3 versions, got %d", len(versions))
	}

	v1, err := docs.GetVersion(id, 1)
	if err != nil {
		t.Fatalf("failed to get version 1: %v", err)
	}
	if v1.Version != 1 {
		t.Errorf("expected version 1, got %d", v1.Version)
	}
	if v1.Title != "v1" {
		t.Errorf("expected title 'v1', got %s", v1.Title)
	}

	v2, err := docs.GetVersion(id, 2)
	if err != nil {
		t.Fatalf("failed to get version 2: %v", err)
	}
	if v2.Version != 2 {
		t.Errorf("expected version 2, got %d", v2.Version)
	}

	v3, err := docs.GetVersion(id, 3)
	if err != nil {
		t.Fatalf("failed to get version 3: %v", err)
	}
	if v3.Version != 3 {
		t.Errorf("expected version 3, got %d", v3.Version)
	}

	current, err := docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get current: %v", err)
	}
	if current.Version != 3 {
		t.Errorf("expected current version 3, got %d", current.Version)
	}
}

func TestVersioningMaxLimit(t *testing.T) {
	file := "test_versioning_max.db"
	defer os.Remove(file)

	db, err := Open(file)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	docs, err := Use[VersionedDoc](db, UseOptions{MaxVersions: 2})
	if err != nil {
		t.Fatalf("failed to use table: %v", err)
	}

	id, err := docs.Insert(&VersionedDoc{Title: "v1", Content: "c1", Version: 1})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	for i := 2; i <= 5; i++ {
		err = docs.Update(id, &VersionedDoc{Title: "v" + string(rune('0'+i)), Content: "c" + string(rune('0'+i)), Version: i})
		if err != nil {
			t.Fatalf("failed to update to v%d: %v", i, err)
		}
	}

	versions, err := docs.ListVersions(id)
	if err != nil {
		t.Fatalf("failed to list versions: %v", err)
	}

	expectedVersions := 3
	if len(versions) != expectedVersions {
		t.Errorf("expected %d versions (current + %d previous), got %d", expectedVersions, 2, len(versions))
	}

	_, err = docs.GetVersion(id, 1)
	if err == nil {
		t.Error("expected error for version 1 (should be cleaned up)")
	}

	_, err = docs.GetVersion(id, 2)
	if err == nil {
		t.Error("expected error for version 2 (should be cleaned up)")
	}

	v3, err := docs.GetVersion(id, 3)
	if err != nil {
		t.Fatalf("failed to get version 3: %v", err)
	}
	if v3.Version != 3 {
		t.Errorf("expected version 3, got %d", v3.Version)
	}

	v4, err := docs.GetVersion(id, 4)
	if err != nil {
		t.Fatalf("failed to get version 4: %v", err)
	}
	if v4.Version != 4 {
		t.Errorf("expected version 4, got %d", v4.Version)
	}

	current, err := docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get current: %v", err)
	}
	if current.Version != 5 {
		t.Errorf("expected current version 5, got %d", current.Version)
	}
}

func TestVersioningPersistence(t *testing.T) {
	file := "test_versioning_persist.db"
	defer os.Remove(file)

	db, err := Open(file)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	docs, err := Use[VersionedDoc](db, UseOptions{MaxVersions: 3})
	if err != nil {
		t.Fatalf("failed to use table: %v", err)
	}

	id, err := docs.Insert(&VersionedDoc{Title: "v1", Content: "c1", Version: 1})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	err = docs.Update(id, &VersionedDoc{Title: "v2", Content: "c2", Version: 2})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	err = docs.Update(id, &VersionedDoc{Title: "v3", Content: "c3", Version: 3})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	versionsBeforeClose, err := docs.ListVersions(id)
	if err != nil {
		t.Fatalf("failed to list versions before close: %v", err)
	}
	numVersionsBefore := len(versionsBeforeClose)

	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close db: %v", err)
	}

	db2, err := Open(file)
	if err != nil {
		t.Fatalf("failed to reopen db: %v", err)
	}
	defer db2.Close()

	docs2, err := Use[VersionedDoc](db2)
	if err != nil {
		t.Fatalf("failed to use table after reopen: %v", err)
	}

	current, err := docs2.Get(id)
	if err != nil {
		t.Fatalf("failed to get current after reopen: %v", err)
	}
	if current.Version != 3 {
		t.Errorf("expected current version 3 after reopen, got %d", current.Version)
	}
	if current.Title != "v3" {
		t.Errorf("expected title 'v3' after reopen, got %s", current.Title)
	}

	versionsAfterReopen, err := docs2.ListVersions(id)
	if err != nil {
		t.Fatalf("list versions after reopen failed: %v", err)
	}

	if len(versionsAfterReopen) != numVersionsBefore {
		t.Errorf("version count changed after reopen (before: %d, after: %d)", numVersionsBefore, len(versionsAfterReopen))
	}
}

func TestVacuumWithoutVersioning(t *testing.T) {
	file := "test_vacuum_no_versioning.db"
	defer os.Remove(file)

	db, err := Open(file)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	docs, err := Use[VersionedDoc](db)
	if err != nil {
		t.Fatalf("failed to use table: %v", err)
	}

	id, err := docs.Insert(&VersionedDoc{Title: "v1", Content: "c1", Version: 1})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	t.Logf("Inserted id=%d", id)

	doc, _ := docs.Get(id)
	t.Logf("Before updates: %+v", doc)

	for i := 2; i <= 4; i++ {
		err = docs.Update(id, &VersionedDoc{ID: id, Title: "v" + string(rune('0'+i)), Content: "c" + string(rune('0'+i)), Version: i})
		if err != nil {
			t.Fatalf("failed to update to v%d: %v", i, err)
		}
	}

	doc, err = docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get before vacuum: %v", err)
	}
	t.Logf("Before vacuum: %+v", doc)

	err = db.Sync()
	if err != nil {
		t.Logf("Sync error: %v", err)
	}

	doc, err = docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get after sync: %v", err)
	}
	t.Logf("After sync: %+v", doc)

	err = db.Vacuum()
	if err != nil {
		t.Fatalf("failed to vacuum: %v", err)
	}
	t.Logf("Vacuum completed")

	doc2, err := docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get after vacuum: %v", err)
	}
	t.Logf("After vacuum: %+v", doc2)
}

func TestVersioningWithVacuum(t *testing.T) {
	file := "test_versioning_vacuum.db"
	defer os.Remove(file)

	db, err := Open(file)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	docs, err := Use[VersionedDoc](db, UseOptions{MaxVersions: 2})
	if err != nil {
		t.Fatalf("failed to use table: %v", err)
	}

	record := &VersionedDoc{Title: "v1", Content: "c1", Version: 1}
	id, err := docs.Insert(record)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	t.Logf("Inserted id=%d, record.ID=%d", id, record.ID)

	doc1, _ := docs.Get(id)
	t.Logf("Got doc after insert: %+v", doc1)

	for i := 2; i <= 4; i++ {
		time.Sleep(1 * time.Millisecond)
		updateRecord := &VersionedDoc{ID: id, Title: "v" + string(rune('0'+i)), Content: "c" + string(rune('0'+i)), Version: i}
		err = docs.Update(id, updateRecord)
		if err != nil {
			t.Fatalf("failed to update to v%d: %v", i, err)
		}
		t.Logf("Updated to version %d", i)

		doc, _ := docs.Get(id)
		t.Logf("Got doc after update %d: %+v", i, doc)
	}

	versionsBefore, err := docs.ListVersions(id)
	if err != nil {
		t.Fatalf("failed to list versions before vacuum: %v", err)
	}
	t.Logf("Versions before vacuum: %d", len(versionsBefore))
	for _, v := range versionsBefore {
		t.Logf("  Version %d at %v", v.Version, v.CreatedAt)
	}

	err = db.Sync()
	if err != nil {
		t.Logf("Sync failed: %v", err)
	}

	currentBefore, err := docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get current before vacuum: %v", err)
	}
	t.Logf("Current before vacuum: %+v", currentBefore)

	err = db.Vacuum()
	if err != nil {
		t.Fatalf("failed to vacuum: %v", err)
	}
	t.Logf("Vacuum completed")

	t.Logf("After vacuum, trying to get id=%d", id)
	current, err := docs.Get(id)
	if err != nil {
		t.Fatalf("failed to get current after vacuum: %v", err)
	}
	if current.Version != 4 {
		t.Errorf("expected current version 4 after vacuum, got %d", current.Version)
	}

	versionsAfter, err := docs.ListVersions(id)
	if err != nil {
		t.Logf("warning: failed to list versions after vacuum: %v", err)
	}
	if versionsAfter != nil {
		t.Logf("Versions after vacuum: %d", len(versionsAfter))
		for _, v := range versionsAfter {
			t.Logf("  Version %d", v.Version)
		}
	}
}

func TestVersioningMultipleRecords(t *testing.T) {
	file := "test_versioning_multi.db"
	defer os.Remove(file)

	db, err := Open(file)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	docs, err := Use[VersionedDoc](db, UseOptions{MaxVersions: 5})
	if err != nil {
		t.Fatalf("failed to use table: %v", err)
	}

	id1, err := docs.Insert(&VersionedDoc{Title: "doc1-v1", Content: "c1", Version: 1})
	if err != nil {
		t.Fatalf("failed to insert doc1: %v", err)
	}

	id2, err := docs.Insert(&VersionedDoc{Title: "doc2-v1", Content: "c1", Version: 1})
	if err != nil {
		t.Fatalf("failed to insert doc2: %v", err)
	}

	err = docs.Update(id1, &VersionedDoc{Title: "doc1-v2", Content: "c2", Version: 2})
	if err != nil {
		t.Fatalf("failed to update doc1: %v", err)
	}

	err = docs.Update(id2, &VersionedDoc{Title: "doc2-v2", Content: "c2", Version: 2})
	if err != nil {
		t.Fatalf("failed to update doc2: %v", err)
	}

	err = docs.Update(id1, &VersionedDoc{Title: "doc1-v3", Content: "c3", Version: 3})
	if err != nil {
		t.Fatalf("failed to update doc1: %v", err)
	}

	versions1, err := docs.ListVersions(id1)
	if err != nil {
		t.Fatalf("failed to list versions for doc1: %v", err)
	}
	if len(versions1) != 3 {
		t.Errorf("expected 3 versionsfor doc1, got %d", len(versions1))
	}

	versions2, err := docs.ListVersions(id2)
	if err != nil {
		t.Fatalf("failed to list versions for doc2: %v", err)
	}
	if len(versions2) != 2 {
		t.Errorf("expected2 versions for doc2, got %d", len(versions2))
	}

	doc1v1, err := docs.GetVersion(id1, 1)
	if err != nil {
		t.Fatalf("failed to get doc1 v1: %v", err)
	}
	if doc1v1.Title != "doc1-v1" {
		t.Errorf("expected doc1 v1 title 'doc1-v1', got %s", doc1v1.Title)
	}

	doc2v1, err := docs.GetVersion(id2, 1)
	if err != nil {
		t.Fatalf("failed to get doc2 v1: %v", err)
	}
	if doc2v1.Title != "doc2-v1" {
		t.Errorf("expected doc2 v1 title 'doc2-v1', got %s", doc2v1.Title)
	}
}
