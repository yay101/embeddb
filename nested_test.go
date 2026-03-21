package embeddb

import (
	"fmt"
	"os"
	"testing"
	"time"

	embedcore "github.com/yay101/embeddbcore"
)

type Address struct {
	City    string `db:"index"`
	ZipCode int    `db:"index"`
}

type Employee struct {
	ID        uint32    `db:"id,primary"`
	Name      string    `db:"index"`
	Age       int       `db:"index"`
	Address   Address   // embedded struct
	CreatedAt time.Time `db:"index"`
}

func TestNestedStructQuery(t *testing.T) {
	dbPath := "test_nested.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	db, err := New[Employee](dbPath, false, true)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// First, let's see what fields are detected
	layout, _ := embedcore.ComputeStructLayout(Employee{})
	fmt.Println("Detected fields:")
	for key, fo := range layout.FieldOffsets {
		fmt.Printf("  Key %d: Name=%s, Type=%v, IsStruct=%v, IsTime=%v\n", key, fo.Name, fo.Type, fo.IsStruct, fo.IsTime)
	}
	fmt.Println()

	// Insert test records
	records := []Employee{
		{
			Name:      "Alice",
			Age:       30,
			Address:   Address{City: "New York", ZipCode: 10001},
			CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			Name:      "Bob",
			Age:       25,
			Address:   Address{City: "Los Angeles", ZipCode: 90001},
			CreatedAt: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			Name:      "Charlie",
			Age:       35,
			Address:   Address{City: "New York", ZipCode: 10002},
			CreatedAt: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			Name:      "Diana",
			Age:       28,
			Address:   Address{City: "Chicago", ZipCode: 60601},
			CreatedAt: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, r := range records {
		id, err := db.Insert(&r)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
		fmt.Printf("Inserted ID %d: %s, City=%s\n", id, r.Name, r.Address.City)
	}
	fmt.Println()

	// Query by nested field
	fmt.Println("Querying Address.City = 'New York'...")
	results, err := db.Query("Address.City", "New York")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Found %d results\n", len(results))
	for _, r := range results {
		fmt.Printf("  - %s, City=%s, Zip=%d\n", r.Name, r.Address.City, r.Address.ZipCode)
	}
	fmt.Println()

	// Query by top-level field
	fmt.Println("Querying Age = 25...")
	results, err = db.Query("Age", 25)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Found %d results\n", len(results))
	for _, r := range results {
		fmt.Printf("  - %s, Age=%d\n", r.Name, r.Age)
	}
	fmt.Println()

	// Query by time
	fmt.Println("Querying CreatedAt = 2024-01-01...")
	results, err = db.Query("CreatedAt", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Found %d results\n", len(results))
	for _, r := range results {
		fmt.Printf("  - %s, CreatedAt=%v\n", r.Name, r.CreatedAt)
	}
}

func TestNestedStructRangeQuery(t *testing.T) {
	dbPath := "test_nested_range.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	db, err := New[Employee](dbPath, false, true)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test records
	records := []Employee{
		{Name: "A", Age: 20, Address: Address{ZipCode: 10000}},
		{Name: "B", Age: 25, Address: Address{ZipCode: 20000}},
		{Name: "C", Age: 30, Address: Address{ZipCode: 30000}},
		{Name: "D", Age: 35, Address: Address{ZipCode: 40000}},
		{Name: "E", Age: 40, Address: Address{ZipCode: 50000}},
	}

	for _, r := range records {
		db.Insert(&r)
	}

	// Query nested field with range
	fmt.Println("Querying Address.ZipCode between 20000 and 40000...")
	results, err := db.QueryRangeBetween("Address.ZipCode", 20000, 40000, true, true)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Found %d results\n", len(results))
	for _, r := range results {
		fmt.Printf("  - %s, Zip=%d\n", r.Name, r.Address.ZipCode)
	}
}
