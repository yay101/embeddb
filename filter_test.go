package embeddb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestFilter(t *testing.T) {
	dbPath := "test_filter.db"
	os.Remove(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := New[TestRecord](dbPath, false, false) // No indexes
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert records with various ages
	ages := []int{18, 25, 30, 35, 40, 45, 50, 55, 60}
	for i, age := range ages {
		record := &TestRecord{
			Name:      fmt.Sprintf("User%d", i),
			Age:       age,
			Score:     float64(age * 10),
			CreatedAt: time.Now(),
		}
		_, err := db.Insert(record)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Filter: Age > 30
	results, err := db.Filter(func(r TestRecord) bool {
		return r.Age > 30
	})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Ages: 18, 25, 30, 35, 40, 45, 50, 55, 60
	// Filter: Age > 30 = 35, 40, 45, 50, 55, 60 = 6 records
	if len(results) != 6 {
		t.Errorf("Expected 6 results, got %d", len(results))
	}

	// Filter: Age >= 30 AND Age <= 50
	results, err = db.Filter(func(r TestRecord) bool {
		return r.Age >= 30 && r.Age <= 50
	})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Ages >= 30 and <= 50 = 30, 35, 40, 45, 50 = 5 records
	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	for _, r := range results {
		if r.Age < 30 || r.Age > 50 {
			t.Errorf("Age %d out of range", r.Age)
		}
	}

	// Filter: Name contains "User1"
	results, err = db.Filter(func(r TestRecord) bool {
		return len(r.Name) > 0 && r.Name[0] == 'U'
	})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) != 9 {
		t.Errorf("Expected 9 results, got %d", len(results))
	}
}

func TestScan(t *testing.T) {
	dbPath := "test_scan.db"
	os.Remove(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := New[TestRecord](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert records
	for i := 0; i < 10; i++ {
		record := &TestRecord{
			Name:  fmt.Sprintf("User%d", i),
			Age:   20 + i,
			Score: float64(i * 10),
		}
		db.Insert(record)
	}

	// Scan and collect all
	allRecords := make([]TestRecord, 0)
	err = db.Scan(func(r TestRecord) bool {
		allRecords = append(allRecords, r)
		return true
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(allRecords) != 10 {
		t.Errorf("Expected 10 records, got %d", len(allRecords))
	}

	// Scan with early exit
	earlyExit := make([]TestRecord, 0)
	err = db.Scan(func(r TestRecord) bool {
		earlyExit = append(earlyExit, r)
		return len(earlyExit) < 3 // Stop after 3 records
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(earlyExit) != 3 {
		t.Errorf("Expected 3 records (early exit), got %d", len(earlyExit))
	}
}

func TestCount(t *testing.T) {
	dbPath := "test_count.db"
	os.Remove(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := New[TestRecord](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	count := db.Count()
	if count != 0 {
		t.Errorf("Expected 0 initial records, got %d", count)
	}

	// Insert some records
	for i := 0; i < 5; i++ {
		db.Insert(&TestRecord{Name: fmt.Sprintf("User%d", i), Age: 20 + i})
	}

	count = db.Count()
	if count != 5 {
		t.Errorf("Expected 5 records, got %d", count)
	}

	// Delete one (soft delete - record stays in index but marked inactive)
	db.Delete(1)

	// Count still returns 5 because soft-deleted records remain in index
	// Use Filter to count only active records
	activeCount, _ := db.Filter(func(r TestRecord) bool { return true })
	count = db.Count()
	if count != 5 {
		t.Errorf("Expected 5 (including deleted), got %d", count)
	}
	if len(activeCount) != 4 {
		t.Errorf("Expected 4 active records, got %d", len(activeCount))
	}
}

func TestFilterWithNestedStruct(t *testing.T) {
	dbPath := "test_filter_nested.db"
	os.Remove(dbPath)
	defer cleanupTestFiles(dbPath)

	type Person struct {
		Name    string
		Address Address
		Age     int
	}

	db, err := New[Person](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert records
	people := []Person{
		{Name: "Alice", Address: Address{City: "New York", ZipCode: 10001}, Age: 30},
		{Name: "Bob", Address: Address{City: "Los Angeles", ZipCode: 90001}, Age: 25},
		{Name: "Charlie", Address: Address{City: "New York", ZipCode: 10002}, Age: 35},
	}

	for _, p := range people {
		db.Insert(&p)
	}

	// Filter by nested struct field
	results, err := db.Filter(func(p Person) bool {
		return p.Address.City == "New York"
	})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Filter by nested int
	results, err = db.Filter(func(p Person) bool {
		return p.Address.ZipCode > 50000
	})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) != 1 || results[0].Name != "Bob" {
		t.Errorf("Expected Bob from LA")
	}
}

func TestFilterWithTime(t *testing.T) {
	dbPath := "test_filter_time.db"
	os.Remove(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := New[TestRecord](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Insert records with different times
	for i := 0; i < 5; i++ {
		db.Insert(&TestRecord{
			Name:      fmt.Sprintf("User%d", i),
			CreatedAt: baseTime.Add(time.Duration(i) * 24 * time.Hour),
		})
	}

	// Filter by time - created after 48 hours from baseTime
	// i=0: day 0, i=1: day 1, i=2: day 2, i=3: day 3, i=4: day 4
	// After 48 hours = after day 2 starts = day 3 and 4 = 2 records
	results, err := db.Filter(func(r TestRecord) bool {
		return r.CreatedAt.After(baseTime.Add(48 * time.Hour))
	})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}
