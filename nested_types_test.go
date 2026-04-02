package embeddb

import (
	"testing"
)

type Contact struct {
	Name  string
	Phone string
}

type Record struct {
	ID       uint32 `db:"id,primary"`
	Name     string
	Contacts []Contact
}

func TestSliceOfStructsFull(t *testing.T) {
	db, _ := Open("/tmp/test_full.db")
	defer db.Close()

	tbl, _ := Use[Record](db, "test")

	rec := &Record{
		Name: "Alice",
		Contacts: []Contact{
			{Name: "Bob", Phone: "555-1234"},
		},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	fetched, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(fetched.Contacts) != 1 {
		t.Fatalf("Expected 1 contact, got %d", len(fetched.Contacts))
	}
	if fetched.Contacts[0].Name != "Bob" {
		t.Fatalf("Expected name 'Bob', got '%s'", fetched.Contacts[0].Name)
	}
	if fetched.Contacts[0].Phone != "555-1234" {
		t.Fatalf("Expected phone '555-1234', got '%s'", fetched.Contacts[0].Phone)
	}
}

func TestSliceOfStructsMultiple(t *testing.T) {
	db, _ := Open("/tmp/test_multi.db")
	defer db.Close()

	tbl, _ := Use[Record](db, "test")

	rec := &Record{
		Name: "Charlie",
		Contacts: []Contact{
			{Name: "Bob", Phone: "555-1234"},
			{Name: "Alice", Phone: "555-5678"},
		},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	fetched, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(fetched.Contacts) != 2 {
		t.Fatalf("Expected 2 contacts, got %d", len(fetched.Contacts))
	}
	if fetched.Contacts[0].Name != "Bob" {
		t.Fatalf("Expected first contact name 'Bob', got '%s'", fetched.Contacts[0].Name)
	}
	if fetched.Contacts[1].Name != "Alice" {
		t.Fatalf("Expected second contact name 'Alice', got '%s'", fetched.Contacts[1].Name)
	}
}

func TestSliceOfStructsEmpty(t *testing.T) {
	db, _ := Open("/tmp/test_empty.db")
	defer db.Close()

	tbl, _ := Use[Record](db, "test")

	rec := &Record{
		Name:     "Empty",
		Contacts: []Contact{},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	fetched, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if fetched.Contacts != nil && len(fetched.Contacts) != 0 {
		t.Fatalf("Expected empty contacts, got %d", len(fetched.Contacts))
	}
}
