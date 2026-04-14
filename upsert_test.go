package embeddb

import (
	"os"
	"testing"
	"time"
)

type LargeDocument struct {
	ID        uint32 `db:"id,primary"`
	Title     string `db:"index"`
	Content   string
	Summary   string
	Metadata  string
	Author    string
	CreatedAt time.Time
	UpdatedAt time.Time
	Tags      []string
}

type IntPKRecord struct {
	ID   int    `db:"id,primary"`
	Name string `db:"index"`
	Data string
}

func TestUpsertWithIntPK(t *testing.T) {
	os.Remove("/tmp/upsert_int_pk.db")
	defer os.Remove("/tmp/upsert_int_pk.db")

	db, err := Open("/tmp/upsert_int_pk.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[IntPKRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}

	_, inserted, err := tbl.Upsert(25235, &IntPKRecord{ID: 25235, Name: "Test Account", Data: "some data"})
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if !inserted {
		t.Error("expected inserted=true for new record")
	}

	rec, err := tbl.Get(25235)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rec.ID != 25235 {
		t.Errorf("expected ID 25235, got %d", rec.ID)
	}
	if rec.Name != "Test Account" {
		t.Errorf("expected Name 'Test Account', got %s", rec.Name)
	}

	_, inserted, err = tbl.Upsert(25235, &IntPKRecord{ID: 25235, Name: "Updated Name", Data: "updated data"})
	if err != nil {
		t.Fatalf("Upsert update failed: %v", err)
	}
	if inserted {
		t.Error("expected inserted=false for existing record")
	}

	rec, err = tbl.Get(25235)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name != "Updated Name" {
		t.Errorf("expected updated name, got %s", rec.Name)
	}

	count := tbl.Count()
	if count != 1 {
		t.Errorf("expected 1 record, got %d", count)
	}
}

func TestUpsertLargeStruct(t *testing.T) {
	os.Remove("/tmp/upsert_large.db")
	defer os.Remove("/tmp/upsert_large.db")

	db, err := Open("/tmp/upsert_large.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	largeContent := makeLargeString(10000)
	largeSummary := makeLargeString(2000)

	// First insert via upsert
	id, inserted, err := tbl.Upsert(uint32(1), &LargeDocument{
		ID:        1,
		Title:     "Initial Title",
		Content:   largeContent,
		Summary:   largeSummary,
		Author:    "Alice",
		CreatedAt: time.Now(),
		Tags:      []string{"draft", "important"},
	})
	if err != nil {
		t.Fatalf("Upsert insert failed: %v", err)
	}
	if !inserted {
		t.Error("expected inserted=true for new record")
	}
	if id != 1 {
		t.Errorf("expected ID 1, got %d", id)
	}

	// Verify first insert
	doc, err := tbl.Get(1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if doc.Title != "Initial Title" {
		t.Errorf("Title mismatch: got %s", doc.Title)
	}
	if len(doc.Content) != 10000 {
		t.Errorf("Content length: got %d, want 10000", len(doc.Content))
	}
	if len(doc.Tags) != 2 {
		t.Errorf("Tags length: got %d, want 2", len(doc.Tags))
	}

	// Upsert again (should update)
	updatedContent := makeLargeString(15000)
	_, inserted, err = tbl.Upsert(uint32(1), &LargeDocument{
		ID:        1,
		Title:     "Updated Title",
		Content:   updatedContent,
		Summary:   "New summary",
		Author:    "Bob",
		UpdatedAt: time.Now(),
		Tags:      []string{"published"},
	})
	if err != nil {
		t.Fatalf("Upsert update failed: %v", err)
	}
	if inserted {
		t.Error("expected inserted=false for existing record")
	}

	// Verify update
	doc, err = tbl.Get(1)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if doc.Title != "Updated Title" {
		t.Errorf("Title after update: got %s, want Updated Title", doc.Title)
	}
	if doc.Author != "Bob" {
		t.Errorf("Author after update: got %s, want Bob", doc.Author)
	}
	if len(doc.Content) != 15000 {
		t.Errorf("Content length after update: got %d, want 15000", len(doc.Content))
	}
	if doc.Tags[0] != "published" {
		t.Errorf("Tags after update: got %v", doc.Tags)
	}
}

func TestUpsertWithIndexes(t *testing.T) {
	os.Remove("/tmp/upsert_indexed.db")
	defer os.Remove("/tmp/upsert_indexed.db")

	db, err := Open("/tmp/upsert_indexed.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert via upsert
	tbl.Upsert(uint32(1), &LargeDocument{ID: 1, Title: "Doc One", Author: "Alice"})
	tbl.Upsert(uint32(2), &LargeDocument{ID: 2, Title: "Doc Two", Author: "Bob"})

	// Query by indexed field
	results, err := tbl.Query("Title", "Doc One")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	// Upsert to update indexed field
	tbl.Upsert(uint32(1), &LargeDocument{ID: 1, Title: "Doc One Updated", Author: "Alice Updated"})

	// Query by old indexed value - should NOT find it after update
	results, err = tbl.Query("Title", "Doc One")
	if err != nil {
		t.Fatalf("Query after update failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for old title after update, got %d", len(results))
	}

	// Query by new indexed value - should find it
	results, err = tbl.Query("Title", "Doc One Updated")
	if err != nil {
		t.Fatalf("Query by new title failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result for new title, got %d", len(results))
	}
	if results[0].Author != "Alice Updated" {
		t.Errorf("Author mismatch: got %s", results[0].Author)
	}
}

func TestUpsertPersistence(t *testing.T) {
	os.Remove("/tmp/upsert_persist.db")
	defer os.Remove("/tmp/upsert_persist.db")

	{
		db, err := Open("/tmp/upsert_persist.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[LargeDocument](db, "docs")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Upsert(uint32(1), &LargeDocument{
			ID:      1,
			Title:   "Persistent Doc",
			Content: makeLargeString(5000),
		})

		db.Close()
	}

	{
		db2, err := Open("/tmp/upsert_persist.db")
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()

		tbl2, err := Use[LargeDocument](db2, "docs")
		if err != nil {
			t.Fatal(err)
		}

		doc, err := tbl2.Get(uint32(1))
		if err != nil {
			t.Fatalf("Get after reopen failed: %v", err)
		}
		if doc.Title != "Persistent Doc" {
			t.Errorf("Title mismatch: got %s", doc.Title)
		}
		if len(doc.Content) != 5000 {
			t.Errorf("Content length: got %d", len(doc.Content))
		}

		// Update via upsert after reopen
		tbl2.Upsert(uint32(1), &LargeDocument{
			ID:      1,
			Title:   "Updated Persistent",
			Content: makeLargeString(8000),
		})

		doc, err = tbl2.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if doc.Title != "Updated Persistent" {
			t.Errorf("Title after update: got %s", doc.Title)
		}
		if len(doc.Content) != 8000 {
			t.Errorf("Content length after update: got %d", len(doc.Content))
		}
	}
}

func TestUpsertMany(t *testing.T) {
	os.Remove("/tmp/upsert_many.db")
	defer os.Remove("/tmp/upsert_many.db")

	db, err := Open("/tmp/upsert_many.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert 50 records
	for i := 1; i <= 50; i++ {
		_, _, err := tbl.Upsert(uint32(i), &LargeDocument{
			ID:      uint32(i),
			Title:   "Doc " + string(rune('A'+i-1)),
			Content: makeLargeString(1000 * i),
		})
		if err != nil {
			t.Fatalf("Upsert %d failed: %v", i, err)
		}
	}

	count := tbl.Count()
	if count != 50 {
		t.Errorf("Expected 50 records, got %d", count)
	}

	// Update all via upsert
	for i := 1; i <= 50; i++ {
		_, inserted, err := tbl.Upsert(uint32(i), &LargeDocument{
			ID:      uint32(i),
			Title:   "Updated Doc " + string(rune('A'+i-1)),
			Content: makeLargeString(2000 * i),
		})
		if err != nil {
			t.Fatalf("Upsert update %d failed: %v", i, err)
		}
		if inserted {
			t.Errorf("Expected inserted=false for record %d", i)
		}
	}

	// Verify all updated
	for i := 1; i <= 50; i++ {
		doc, err := tbl.Get(uint32(i))
		if err != nil {
			t.Fatalf("Get %d failed: %v", i, err)
		}
		expectedTitle := "Updated Doc " + string(rune('A'+i-1))
		if doc.Title != expectedTitle {
			t.Errorf("Record %d: got %s, want %s", i, doc.Title, expectedTitle)
		}
	}
}

func TestDeleteWithIndexes(t *testing.T) {
	os.Remove("/tmp/delete_indexed.db")
	defer os.Remove("/tmp/delete_indexed.db")

	db, err := Open("/tmp/delete_indexed.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert records
	tbl.Insert(&LargeDocument{ID: 1, Title: "First", Author: "Alice"})
	tbl.Insert(&LargeDocument{ID: 2, Title: "Second", Author: "Bob"})
	tbl.Insert(&LargeDocument{ID: 3, Title: "Third", Author: "Charlie"})

	// Verify queries work
	results, _ := tbl.Query("Title", "First")
	if len(results) != 1 {
		t.Fatalf("Expected 1 result for First, got %d", len(results))
	}

	// Delete a record
	err = tbl.Delete(uint32(1))
	if err != nil {
		t.Fatal(err)
	}

	// Query should not find deleted record
	results, _ = tbl.Query("Title", "First")
	if len(results) != 0 {
		t.Errorf("Expected 0 results after delete, got %d", len(results))
	}

	// Other records should still be findable
	results, _ = tbl.Query("Title", "Second")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for Second, got %d", len(results))
	}
}

func TestDeleteManyWithIndexes(t *testing.T) {
	os.Remove("/tmp/delete_many_indexed.db")
	defer os.Remove("/tmp/delete_many_indexed.db")

	db, err := Open("/tmp/delete_many_indexed.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert records
	for i := 1; i <= 10; i++ {
		tbl.Insert(&LargeDocument{ID: uint32(i), Title: string(rune('A' + i - 1)), Author: "User"})
	}

	// Delete multiple
	deleted, err := tbl.DeleteMany([]any{uint32(1), uint32(3), uint32(5)})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Errorf("Expected 3 deleted, got %d", deleted)
	}

	// Verify deleted are gone
	for _, id := range []uint32{1, 3, 5} {
		_, err := tbl.Get(id)
		if err == nil {
			t.Errorf("Expected record %d to be deleted", id)
		}
	}

	// Verify remaining
	for _, id := range []uint32{2, 4, 6, 7, 8, 9, 10} {
		_, err := tbl.Get(id)
		if err != nil {
			t.Errorf("Expected record %d to exist, got error: %v", id, err)
		}
	}

	// Verify indexes cleaned up
	results, _ := tbl.Query("Title", "A")
	if len(results) != 0 {
		t.Errorf("Expected 0 results for deleted A, got %d", len(results))
	}
	results, _ = tbl.Query("Title", "B")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for B, got %d", len(results))
	}
}

func makeLargeString(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}
