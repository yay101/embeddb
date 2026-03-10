package embeddb

import (
	"testing"
)

type SliceRecord struct {
	ID     uint32   `db:"id,primary"`
	Name   string   `db:"index"`
	Tags   []string `db:"index"`
	Scores []int    `db:"index"`
}

func TestSliceStringIndexing(t *testing.T) {
	dbPath := "test_slice_db"
	defer cleanupTestFiles(dbPath)

	db, err := New[SliceRecord](dbPath, false, true)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	records := []SliceRecord{
		{ID: 1, Name: "Alice", Tags: []string{"admin", "developer"}},
		{ID: 2, Name: "Bob", Tags: []string{"user", "developer"}},
		{ID: 3, Name: "Charlie", Tags: []string{"admin", "manager"}},
		{ID: 4, Name: "Diana", Tags: []string{"user"}},
	}

	for _, record := range records {
		_, err := db.Insert(&record)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	results, err := db.Query("Tags", "admin")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results for 'admin' tag, got %d", len(results))
	}

	found := false
	for _, r := range results {
		if r.Name == "Alice" || r.Name == "Charlie" {
			found = true
		}
	}
	if !found {
		t.Error("Did not find expected records with admin tag")
	}

	results2, err := db.Query("Tags", "developer")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results2) != 2 {
		t.Fatalf("Expected 2 results for 'developer' tag, got %d", len(results2))
	}
}

func TestSliceIntIndexing(t *testing.T) {
	dbPath := "test_slice_int_db"
	defer cleanupTestFiles(dbPath)

	db, err := New[SliceRecord](dbPath, false, true)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	records := []SliceRecord{
		{ID: 1, Name: "Alice", Scores: []int{10, 20, 30}},
		{ID: 2, Name: "Bob", Scores: []int{15, 25}},
		{ID: 3, Name: "Charlie", Scores: []int{10, 35}},
		{ID: 4, Name: "Diana", Scores: []int{20, 40}},
	}

	for _, record := range records {
		_, err := db.Insert(&record)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	results, err := db.Query("Scores", 10)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results for score 10, got %d", len(results))
	}

	results2, err := db.Query("Scores", 20)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results2) != 2 {
		t.Fatalf("Expected 2 results for score 20, got %d", len(results2))
	}
}

func TestSliceInsertAndRetrieve(t *testing.T) {
	dbPath := "test_slice_retrieve_db"
	defer cleanupTestFiles(dbPath)

	db, err := New[SliceRecord](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	record := SliceRecord{
		ID:     1,
		Name:   "Test",
		Tags:   []string{"one", "two", "three"},
		Scores: []int{100, 200, 300},
	}

	id, err := db.Insert(&record)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	retrieved, err := db.Get(id)
	if err != nil {
		t.Fatalf("Failed to get record: %v", err)
	}

	if len(retrieved.Tags) != 3 {
		t.Fatalf("Expected 3 tags, got %d", len(retrieved.Tags))
	}

	if retrieved.Tags[0] != "one" || retrieved.Tags[1] != "two" || retrieved.Tags[2] != "three" {
		t.Errorf("Tags mismatch: got %v", retrieved.Tags)
	}

	if len(retrieved.Scores) != 3 {
		t.Fatalf("Expected 3 scores, got %d", len(retrieved.Scores))
	}

	if retrieved.Scores[0] != 100 || retrieved.Scores[1] != 200 || retrieved.Scores[2] != 300 {
		t.Errorf("Scores mismatch: got %v", retrieved.Scores)
	}
}
