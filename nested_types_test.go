package embeddb

import (
	"testing"
)

// ─── Basic Slice Types ───────────────────────────────────────────────────────

type StringSliceRecord struct {
	ID     uint32 `db:"id,primary"`
	Names  []string
	Phones []string
}

type IntSliceRecord struct {
	ID     uint32 `db:"id,primary"`
	Nums   []int
	Scores []int
}

// ─── Slices of Structs ───────────────────────────────────────────────────────

type Contact struct {
	Name  string
	Phone string
}

type Location struct {
	Street string
	City   string
}

type PersonWithContacts struct {
	ID       uint32 `db:"id,primary"`
	Name     string
	Contacts []Contact
}

type PersonWithLocations struct {
	ID        uint32 `db:"id,primary"`
	Name      string
	Locations []Location
}

// ─── Structs with Multiple Slices ─────────────────────────────────────────────

type ComplexPerson struct {
	ID       uint32 `db:"id,primary"`
	Name     string
	Contacts []Contact
	Tags     []string
	Numbers  []int
}

// ─── Nested Structs (named) ──────────────────────────────────────────────────

type InnerStruct struct {
	Value  string
	Number int
}

type OuterWithNested struct {
	ID    uint32 `db:"id,primary"`
	Name  string
	Inner InnerStruct
}

// ─── Anonymous (embedded) Structs ─────────────────────────────────────────────

type Embedded struct {
	City    string
	Country string
}

type WithEmbedded struct {
	ID   uint32 `db:"id,primary"`
	Name string
	Embedded
}

// ─── Nested Slices in Named Struct ────────────────────────────────────────────

type ContactInfo struct {
	Phone string
	Email string
}

type Profile struct {
	Name        string
	ContactInfo ContactInfo
	Tags        []string
	Contacts    []Contact
}

type PersonWithProfile struct {
	ID      uint32 `db:"id,primary"`
	Profile Profile
}

// ─── Edge Cases ──────────────────────────────────────────────────────────────

type EmptyStruct struct{}

type WithEmptyStruct struct {
	ID    uint32 `db:"id,primary"`
	Name  string
	Empty EmptyStruct
}

// ─── String Slice Tests ──────────────────────────────────────────────────────

func TestStringSliceBasic(t *testing.T) {
	db, _ := Open("/tmp/test_str_slice_basic.db")
	defer db.Close()
	tbl, _ := Use[StringSliceRecord](db, "test")

	rec := &StringSliceRecord{
		Names:  []string{"Alice", "Bob"},
		Phones: []string{"555-1234", "555-5678"},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	fetched, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(fetched.Names) != 2 || fetched.Names[0] != "Alice" || fetched.Names[1] != "Bob" {
		t.Fatalf("Names mismatch: got %v, want [Alice Bob]", fetched.Names)
	}
	if len(fetched.Phones) != 2 || fetched.Phones[0] != "555-1234" || fetched.Phones[1] != "555-5678" {
		t.Fatalf("Phones mismatch: got %v, want [555-1234 555-5678]", fetched.Phones)
	}
}

func TestStringSliceEmpty(t *testing.T) {
	db, _ := Open("/tmp/test_str_slice_empty.db")
	defer db.Close()
	tbl, _ := Use[StringSliceRecord](db, "test")

	rec := &StringSliceRecord{Names: []string{}}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	fetched, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if fetched.Names != nil && len(fetched.Names) != 0 {
		t.Fatalf("Expected empty names, got %v", fetched.Names)
	}
}

// ─── Int Slice Tests ─────────────────────────────────────────────────────────

func TestIntSliceBasic(t *testing.T) {
	db, _ := Open("/tmp/test_int_slice_basic.db")
	defer db.Close()
	tbl, _ := Use[IntSliceRecord](db, "test")

	rec := &IntSliceRecord{
		Nums:   []int{1, 2, 3},
		Scores: []int{100, 95, 87},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	fetched, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(fetched.Nums) != 3 || fetched.Nums[0] != 1 || fetched.Nums[1] != 2 || fetched.Nums[2] != 3 {
		t.Fatalf("Nums mismatch: got %v", fetched.Nums)
	}
	if len(fetched.Scores) != 3 || fetched.Scores[0] != 100 || fetched.Scores[1] != 95 || fetched.Scores[2] != 87 {
		t.Fatalf("Scores mismatch: got %v", fetched.Scores)
	}
}

// ─── Slice of Structs Tests ──────────────────────────────────────────────────

func TestSliceOfStructsSingleElement(t *testing.T) {
	db, _ := Open("/tmp/test_struct_slice_single.db")
	defer db.Close()
	tbl, _ := Use[PersonWithContacts](db, "test")

	rec := &PersonWithContacts{
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
		t.Fatalf("Expected contact name 'Bob', got '%s'", fetched.Contacts[0].Name)
	}
	if fetched.Contacts[0].Phone != "555-1234" {
		t.Fatalf("Expected contact phone '555-1234', got '%s'", fetched.Contacts[0].Phone)
	}
}

func TestSliceOfStructsMultipleElements(t *testing.T) {
	db, _ := Open("/tmp/test_struct_slice_multi.db")
	defer db.Close()
	tbl, _ := Use[PersonWithContacts](db, "test")

	rec := &PersonWithContacts{
		Name: "Charlie",
		Contacts: []Contact{
			{Name: "Bob", Phone: "555-1234"},
			{Name: "Alice", Phone: "555-5678"},
			{Name: "Dave", Phone: "555-9012"},
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

	if len(fetched.Contacts) != 3 {
		t.Fatalf("Expected 3 contacts, got %d", len(fetched.Contacts))
	}

	for i, wantName := range []string{"Bob", "Alice", "Dave"} {
		if fetched.Contacts[i].Name != wantName {
			t.Fatalf("Contact[%d].Name = %s, want %s", i, fetched.Contacts[i].Name, wantName)
		}
	}
}

func TestSliceOfStructsEmpty(t *testing.T) {
	db, _ := Open("/tmp/test_struct_slice_empty.db")
	defer db.Close()
	tbl, _ := Use[PersonWithContacts](db, "test")

	rec := &PersonWithContacts{
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

func TestSliceOfStructsDifferentTypes(t *testing.T) {
	db, _ := Open("/tmp/test_struct_slice_types.db")
	defer db.Close()

	// Test with Location type
	tbl, _ := Use[PersonWithLocations](db, "test")

	rec := &PersonWithLocations{
		Name: "Test",
		Locations: []Location{
			{Street: "123 Main St", City: "NYC"},
			{Street: "456 Oak Ave", City: "LA"},
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

	if len(fetched.Locations) != 2 {
		t.Fatalf("Expected 2 locations, got %d", len(fetched.Locations))
	}
	if fetched.Locations[0].City != "NYC" || fetched.Locations[1].City != "LA" {
		t.Fatalf("Location cities mismatch: %v", fetched.Locations)
	}
}

// ─── Multiple Slices in Same Record ──────────────────────────────────────────

func TestMultipleSlices(t *testing.T) {
	db, _ := Open("/tmp/test_multi_slices.db")
	defer db.Close()
	tbl, _ := Use[ComplexPerson](db, "test")

	rec := &ComplexPerson{
		Name: "Complex",
		Contacts: []Contact{
			{Name: "Bob", Phone: "555-1234"},
		},
		Tags:    []string{"admin", "user"},
		Numbers: []int{1, 2, 3},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	fetched, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(fetched.Contacts) != 1 || fetched.Contacts[0].Name != "Bob" {
		t.Fatalf("Contacts mismatch: %v", fetched.Contacts)
	}
	if len(fetched.Tags) != 2 || fetched.Tags[0] != "admin" {
		t.Fatalf("Tags mismatch: %v", fetched.Tags)
	}
	if len(fetched.Numbers) != 3 || fetched.Numbers[0] != 1 {
		t.Fatalf("Numbers mismatch: %v", fetched.Numbers)
	}
}

// ─── Nested Structs ──────────────────────────────────────────────────────────

func TestNamedNestedStruct(t *testing.T) {
	db, _ := Open("/tmp/test_named_nested.db")
	defer db.Close()
	tbl, _ := Use[OuterWithNested](db, "test")

	rec := &OuterWithNested{
		Name: "Test",
		Inner: InnerStruct{
			Value:  "inner_value",
			Number: 42,
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

	if fetched.Inner.Value != "inner_value" {
		t.Fatalf("Inner.Value mismatch: got '%s', want 'inner_value'", fetched.Inner.Value)
	}
	if fetched.Inner.Number != 42 {
		t.Fatalf("Inner.Number mismatch: got %d, want 42", fetched.Inner.Number)
	}
}

// ─── Embedded Structs ────────────────────────────────────────────────────────

func TestEmbeddedStruct(t *testing.T) {
	db, _ := Open("/tmp/test_embedded.db")
	defer db.Close()
	tbl, _ := Use[WithEmbedded](db, "test")

	rec := &WithEmbedded{
		Name: "Test",
		Embedded: Embedded{
			City:    "NYC",
			Country: "USA",
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

	// Embedded struct fields should be promoted
	if fetched.City != "NYC" {
		t.Fatalf("City mismatch: got '%s', want 'NYC'", fetched.City)
	}
	if fetched.Country != "USA" {
		t.Fatalf("Country mismatch: got '%s', want 'USA'", fetched.Country)
	}
}

// ─── Full Schema Tests ───────────────────────────────────────────────────────

func TestFullSchema(t *testing.T) {
	db, _ := Open("/tmp/test_full_schema.db")
	defer db.Close()

	tbl, _ := Use[PersonWithProfile](db, "test")

	rec := &PersonWithProfile{
		Profile: Profile{
			Name: "John Doe",
			ContactInfo: ContactInfo{
				Phone: "555-1234",
				Email: "john@example.com",
			},
			Tags: []string{"vip", "premium"},
			Contacts: []Contact{
				{Name: "Jane", Phone: "555-9999"},
			},
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

	// Check nested struct
	if fetched.Profile.Name != "John Doe" {
		t.Fatalf("Profile.Name mismatch: got '%s'", fetched.Profile.Name)
	}
	if fetched.Profile.ContactInfo.Phone != "555-1234" {
		t.Fatalf("Profile.ContactInfo.Phone mismatch: got '%s'", fetched.Profile.ContactInfo.Phone)
	}

	// Check nested slices
	if len(fetched.Profile.Tags) != 2 || fetched.Profile.Tags[0] != "vip" {
		t.Fatalf("Profile.Tags mismatch: %v", fetched.Profile.Tags)
	}
	if len(fetched.Profile.Contacts) != 1 || fetched.Profile.Contacts[0].Name != "Jane" {
		t.Fatalf("Profile.Contacts mismatch: %v", fetched.Profile.Contacts)
	}
}
