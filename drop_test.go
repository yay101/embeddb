package embeddb

import (
	"os"
	"testing"
)

func TestTableDrop(t *testing.T) {
	removeFile := func(name string) { os.Remove(name) }
	fn := "/tmp/drop_test.db"
	removeFile(fn)
	defer removeFile(fn)

	db, err := New[PerfUser](fn, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users, err := db.Table("users")
	if err != nil {
		t.Fatal(err)
	}

	// Insert some records
	for i := 0; i < 10; i++ {
		_, err := users.Insert(&PerfUser{
			Name:     "User",
			Age:      20,
			City:     "City",
			Category: "Cat",
			Status:   "active",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	count := users.Count()
	if count != 10 {
		t.Errorf("Expected 10 records, got %d", count)
	}

	// Drop the table
	if err := users.Drop(); err != nil {
		t.Fatal(err)
	}

	// Verify table is marked as dropped
	if !users.IsDropped() {
		t.Error("Expected table to be marked as dropped")
	}

	// Verify catalog entry
	entry, ok := db.tableCatalog.GetTable("users")
	if !ok {
		t.Fatal("Table not found in catalog")
	}
	if !entry.Dropped {
		t.Error("Expected catalog entry to show dropped")
	}

	t.Log("Table drop functionality works correctly")
}
