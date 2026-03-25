package embeddb

import (
	"fmt"
	"os"
	"testing"
)

type Person struct {
	Name string
	Age  int `db:"index"`
}

func TestQueryPaged(t *testing.T) {
	dbPath := "/tmp/test_paged.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[Person](dbPath, false, true) // autoIndex=true
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer db.Close()

	// Insert 25 records with ages that have some duplicates
	for i := 1; i <= 25; i++ {
		person := Person{Name: "Person", Age: (i % 5) + 20} // Ages: 20,21,22,23,24,20,21,22,23,24...
		_, err := db.Insert(&person)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Create index on Age
	if err := db.CreateIndex("Age"); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Test QueryRangeBetweenPaged - get ages 20-22
	result, err := db.QueryRangeBetweenPaged("Age", 20, 22, true, true, 0, 5)
	if err != nil {
		t.Fatalf("QueryRangeBetweenPaged failed: %v", err)
	}

	// Should match ages 20,21,22 which appears 15 times (3 ages * 5 times each) = 15 total
	if result.TotalCount != 15 {
		t.Errorf("expected total count 15, got %d", result.TotalCount)
	}
	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}
	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}

	// Verify all returned records have age 20-22
	for _, rec := range result.Records {
		if rec.Age < 20 || rec.Age > 22 {
			t.Errorf("expected age 20-22, got %d", rec.Age)
		}
	}

	// Get next page
	result, err = db.QueryRangeBetweenPaged("Age", 20, 22, true, true, 5, 5)
	if err != nil {
		t.Fatalf("QueryRangeBetweenPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}

	// Get final page
	result, err = db.QueryRangeBetweenPaged("Age", 20, 22, true, true, 10, 5)
	if err != nil {
		t.Fatalf("QueryRangeBetweenPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}

	// Get beyond bounds
	result, err = db.QueryRangeBetweenPaged("Age", 20, 22, true, true, 15, 5)
	if err != nil {
		t.Fatalf("QueryRangeBetweenPaged failed: %v", err)
	}

	if len(result.Records) != 0 {
		t.Errorf("expected 0 records, got %d", len(result.Records))
	}
	if result.HasMore {
		t.Error("expected HasMore to be false")
	}
}

func TestQueryRangeGreaterThanPaged(t *testing.T) {
	dbPath := "/tmp/test_paged_gt.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[Person](dbPath, false, true) // autoIndex=true
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer db.Close()

	// Insert 20 records with ages 1-20
	for i := 1; i <= 20; i++ {
		person := Person{Name: "Person", Age: i}
		_, err := db.Insert(&person)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	if err := db.CreateIndex("Age"); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Get ages > 10, page 1 with limit 5
	result, err := db.QueryRangeGreaterThanPaged("Age", 10, false, 0, 5)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThanPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}
	if result.TotalCount != 10 { // ages 11-20 = 10 records
		t.Errorf("expected total count 10, got %d", result.TotalCount)
	}
	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}

	// Verify all returned records have age > 10
	for _, rec := range result.Records {
		if rec.Age <= 10 {
			t.Errorf("expected age > 10, got %d", rec.Age)
		}
	}

	// Get page 2
	result, err = db.QueryRangeGreaterThanPaged("Age", 10, false, 5, 5)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThanPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}
	if result.HasMore {
		t.Error("expected HasMore to be false")
	}

	// Verify all records on page 2 have age > 10
	for _, rec := range result.Records {
		if rec.Age <= 10 {
			t.Errorf("expected age > 10, got %d", rec.Age)
		}
	}
}

func TestFilterPaged(t *testing.T) {
	dbPath := "/tmp/test_paged_filter.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[Person](dbPath, false, false)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer db.Close()

	table, err := db.Table("people")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert 20 records with different ages
	for i := 1; i <= 20; i++ {
		person := Person{Name: "Person", Age: i}
		_, err := table.Insert(&person)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Filter for ages >= 15
	result, err := table.FilterPaged(func(p Person) bool {
		return p.Age >= 15
	}, 0, 5)
	if err != nil {
		t.Fatalf("FilterPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}
	if result.TotalCount != 6 { // ages 15, 16, 17, 18, 19, 20
		t.Errorf("expected total count 6, got %d", result.TotalCount)
	}
	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}

	// Verify all returned records match filter and are >= 15
	for _, rec := range result.Records {
		if rec.Age < 15 {
			t.Errorf("expected age >= 15, got %d", rec.Age)
		}
	}

	// Get second page
	result, err = table.FilterPaged(func(p Person) bool {
		return p.Age >= 15
	}, 5, 5)
	if err != nil {
		t.Fatalf("FilterPaged failed: %v", err)
	}

	if len(result.Records) != 1 {
		t.Errorf("expected 1 record, got %d", len(result.Records))
	}
	if result.HasMore {
		t.Error("expected HasMore to be false")
	}
	if result.Records[0].Age < 15 {
		t.Errorf("expected age >= 15, got %d", result.Records[0].Age)
	}
}

func TestTablePaged(t *testing.T) {
	dbPath := "/tmp/test_table_paged.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[Person](dbPath, false, false)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer db.Close()

	table, err := db.Table("people")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert 15 records
	for i := 1; i <= 15; i++ {
		person := Person{Name: "Person", Age: i}
		_, err := table.Insert(&person)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Test FilterPaged on table
	result, err := table.FilterPaged(func(p Person) bool {
		return p.Age <= 10
	}, 0, 5)
	if err != nil {
		t.Fatalf("FilterPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}
	if result.TotalCount != 10 {
		t.Errorf("expected total count 10, got %d", result.TotalCount)
	}

	// Verify all returned records match filter
	for _, rec := range result.Records {
		if rec.Age > 10 {
			t.Errorf("expected age <= 10, got %d", rec.Age)
		}
	}
}

func TestAll(t *testing.T) {
	dbPath := "/tmp/test_all.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[Person](dbPath, false, false)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer db.Close()

	table, err := db.Table("people")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert 10 records
	for i := 1; i <= 10; i++ {
		person := Person{Name: fmt.Sprintf("Person%d", i), Age: i * 10}
		_, err := table.Insert(&person)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Get all records
	all, err := table.All()
	if err != nil {
		t.Fatalf("All failed: %v", err)
	}

	if len(all) != 10 {
		t.Errorf("expected 10 records, got %d", len(all))
	}
}

func TestAllPaged(t *testing.T) {
	dbPath := "/tmp/test_all_paged.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[Person](dbPath, false, false)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer db.Close()

	table, err := db.Table("people")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert 20 records
	for i := 1; i <= 20; i++ {
		person := Person{Name: fmt.Sprintf("Person%d", i), Age: i}
		_, err := table.Insert(&person)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Get first page
	result, err := table.AllPaged(0, 5)
	if err != nil {
		t.Fatalf("AllPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}
	if result.TotalCount != 20 {
		t.Errorf("expected total count 20, got %d", result.TotalCount)
	}
	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}

	// Get second page
	result, err = table.AllPaged(5, 5)
	if err != nil {
		t.Fatalf("AllPaged failed: %v", err)
	}

	if len(result.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(result.Records))
	}

	// Get beyond bounds
	result, err = table.AllPaged(20, 5)
	if err != nil {
		t.Fatalf("AllPaged failed: %v", err)
	}

	if len(result.Records) != 0 {
		t.Errorf("expected 0 records, got %d", len(result.Records))
	}
	if result.HasMore {
		t.Error("expected HasMore to be false")
	}
}
