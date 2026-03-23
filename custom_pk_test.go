package embeddb

import (
	"fmt"
	"os"
	"testing"
)

// Test structs with different PK types

type UserWithStringPK struct {
	Email string `db:"id,primary"`
	Name  string
	Age   int
}

type UserWithIntPK struct {
	SKU  int `db:"id,primary"`
	Name string
}

type UserWithNoPK struct {
	ID   uint32
	Name string
}

type UserWithIgnoredField struct {
	ID       uint32 `db:"id,primary"`
	Name     string
	Cache    []byte `db:"-"`
	Internal string `db:"-"`
}

type UserWithUint32PK struct {
	ID   uint32 `db:"id,primary"`
	Name string
}

func TestStringPrimaryKey(t *testing.T) {
	dbPath := "test_string_pk.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithStringPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Verify PK index was auto-created
	if db.indexManager == nil {
		t.Fatal("expected index manager")
	}
	if !db.indexManager.HasIndex("Email") {
		t.Fatal("expected Email index to be auto-created")
	}

	// Insert records
	user1 := &UserWithStringPK{Email: "alice@example.com", Name: "Alice", Age: 30}
	id1, err := db.Insert(user1)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	t.Logf("Inserted user with internal ID: %d", id1)

	// Insert another user
	user2 := &UserWithStringPK{Email: "bob@example.com", Name: "Bob", Age: 25}
	_, err = db.Insert(user2)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Get by email
	found, err := db.Get("alice@example.com")
	if err != nil {
		t.Fatalf("failed to get by email: %v", err)
	}
	if found.Email != "alice@example.com" {
		t.Errorf("expected email alice@example.com, got %s", found.Email)
	}
	if found.Name != "Alice" {
		t.Errorf("expected name Alice, got %s", found.Name)
	}

	// Update by email
	user1.Name = "Alice Updated"
	err = db.Update("alice@example.com", user1)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Verify update
	found, err = db.Get("alice@example.com")
	if err != nil {
		t.Fatalf("failed to get after update: %v", err)
	}
	if found.Name != "Alice Updated" {
		t.Errorf("expected name Alice Updated, got %s", found.Name)
	}

	// Delete by email
	err = db.Delete("bob@example.com")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify delete
	_, err = db.Get("bob@example.com")
	if err == nil {
		t.Error("expected error getting deleted record")
	}
}

func TestIntPrimaryKey(t *testing.T) {
	dbPath := "test_int_pk.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithIntPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Verify PK index was auto-created
	if !db.indexManager.HasIndex("SKU") {
		t.Fatal("expected SKU index to be auto-created")
	}

	// Insert records
	prod1 := &UserWithIntPK{SKU: 12345, Name: "Product A"}
	_, err = db.Insert(prod1)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	prod2 := &UserWithIntPK{SKU: 67890, Name: "Product B"}
	_, err = db.Insert(prod2)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Get by SKU (int)
	found, err := db.Get(12345)
	if err != nil {
		t.Fatalf("failed to get by SKU: %v", err)
	}
	if found.Name != "Product A" {
		t.Errorf("expected Product A, got %s", found.Name)
	}

	// Get by SKU (int - same as field type)
	found, err = db.Get(67890)
	if err != nil {
		t.Fatalf("failed to get by SKU (int): %v", err)
	}
	if found.Name != "Product B" {
		t.Errorf("expected Product B, got %s", found.Name)
	}

	// Update by SKU
	prod1.Name = "Product A Updated"
	err = db.Update(12345, prod1)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Verify update
	found, err = db.Get(12345)
	if err != nil {
		t.Fatalf("failed to get after update: %v", err)
	}
	if found.Name != "Product A Updated" {
		t.Errorf("expected Product A Updated, got %s", found.Name)
	}

	// Delete by SKU
	err = db.Delete(67890)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify delete
	_, err = db.Get(67890)
	if err == nil {
		t.Error("expected error getting deleted record")
	}
}

func TestIgnoredField(t *testing.T) {
	dbPath := "test_ignored.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithIgnoredField](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Insert with ignored fields set
	user := &UserWithIgnoredField{
		ID:       0, // Will be auto-generated
		Name:     "Test User",
		Cache:    []byte("cached data"),
		Internal: "internal data",
	}
	_, err = db.Insert(user)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Get the record
	found, err := db.Get(uint32(1))
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	// Verify ignored fields are zeroed
	if len(found.Cache) != 0 {
		t.Errorf("expected empty Cache, got %v", found.Cache)
	}
	if found.Internal != "" {
		t.Errorf("expected empty Internal, got %s", found.Internal)
	}

	// Verify regular field is preserved
	if found.Name != "Test User" {
		t.Errorf("expected Test User, got %s", found.Name)
	}
}

func TestUint32PrimaryKey(t *testing.T) {
	dbPath := "test_uint32_pk.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithUint32PK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// For uint32 PKs, the ID is auto-generated by the database
	// The user's ID field value is ignored
	user1 := &UserWithUint32PK{ID: 100, Name: "User 100"}
	id1, err := db.Insert(user1)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	t.Logf("Inserted user, returned ID: %d, user.ID: %d", id1, user1.ID)

	// Note: user1.ID is still 100 (not updated) - the user's struct is not modified
	// But when we decode a record, the ID field is populated from the header

	// Get by the auto-generated ID
	found, err := db.Get(id1)
	if err != nil {
		t.Fatalf("failed to get by ID: %v", err)
	}
	if found.Name != "User 100" {
		t.Errorf("expected User 100, got %s", found.Name)
	}
	// The decoded record should have the ID from the header (not user's 100)
	if found.ID != id1 {
		t.Errorf("expected decoded ID to be %d, got %d", id1, found.ID)
	}
}

func TestBackwardCompatibility(t *testing.T) {
	dbPath := "test_compat.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithNoPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Insert without any PK tag
	user := &UserWithNoPK{ID: 0, Name: "Test User"}
	id, err := db.Insert(user)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Get by uint32
	found, err := db.Get(id)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if found.Name != "Test User" {
		t.Errorf("expected Test User, got %s", found.Name)
	}

	// Update by uint32
	user.Name = "Updated User"
	err = db.Update(id, user)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Delete by uint32
	err = db.Delete(id)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
}

func TestTableStringPK(t *testing.T) {
	dbPath := "test_table_string_pk.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithStringPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	table, err := db.Table()
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert via table
	user := &UserWithStringPK{Email: "test@example.com", Name: "Test User", Age: 25}
	_, err = table.Insert(user)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Get by email via table
	found, err := table.Get("test@example.com")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if found.Name != "Test User" {
		t.Errorf("expected Test User, got %s", found.Name)
	}

	// Update via table
	user.Name = "Updated"
	err = table.Update("test@example.com", user)
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Delete via table
	err = table.Delete("test@example.com")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
}

func TestMultipleStringPK(t *testing.T) {
	dbPath := "test_multi_string_pk.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithStringPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Insert multiple records
	emails := []string{
		"alice@example.com",
		"bob@example.com",
		"charlie@example.com",
		"david@example.com",
		"eve@example.com",
	}
	for i, email := range emails {
		user := &UserWithStringPK{Email: email, Name: fmt.Sprintf("User %d", i), Age: 20 + i}
		_, err := db.Insert(user)
		if err != nil {
			t.Fatalf("failed to insert %s: %v", email, err)
		}
	}

	// Query each one
	for i, email := range emails {
		found, err := db.Get(email)
		if err != nil {
			t.Fatalf("failed to get %s: %v", email, err)
		}
		expected := fmt.Sprintf("User %d", i)
		if found.Name != expected {
			t.Errorf("expected %s, got %s", expected, found.Name)
		}
	}

	// Verify all are indexed
	results, err := db.indexManager.Query("Email", "charlie@example.com")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestUpsertStringPK(t *testing.T) {
	dbPath := "test_upsert.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithStringPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Insert new record via Upsert
	id, inserted, err := db.Upsert("alice@example.com", &UserWithStringPK{Email: "alice@example.com", Name: "Alice", Age: 30})
	if err != nil {
		t.Fatalf("failed to upsert: %v", err)
	}
	if !inserted {
		t.Error("expected inserted to be true")
	}
	t.Logf("Inserted with internal ID: %d", id)

	// Verify inserted
	found, err := db.Get("alice@example.com")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if found.Name != "Alice" {
		t.Errorf("expected Alice, got %s", found.Name)
	}

	// Upsert existing record
	id2, inserted2, err := db.Upsert("alice@example.com", &UserWithStringPK{Email: "alice@example.com", Name: "Alice Updated", Age: 31})
	if err != nil {
		t.Fatalf("failed to upsert update: %v", err)
	}
	if inserted2 {
		t.Error("expected inserted to be false for update")
	}
	if id != id2 {
		t.Errorf("expected same internal ID: %d vs %d", id, id2)
	}

	// Verify updated
	found, err = db.Get("alice@example.com")
	if err != nil {
		t.Fatalf("failed to get after update: %v", err)
	}
	if found.Name != "Alice Updated" {
		t.Errorf("expected Alice Updated, got %s", found.Name)
	}
	if found.Age != 31 {
		t.Errorf("expected age 31, got %d", found.Age)
	}
}

func TestUpsertIntPK(t *testing.T) {
	dbPath := "test_upsert_int.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithIntPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Insert via Upsert
	_, inserted, err := db.Upsert(12345, &UserWithIntPK{SKU: 12345, Name: "Product A"})
	if err != nil {
		t.Fatalf("failed to upsert: %v", err)
	}
	if !inserted {
		t.Error("expected inserted to be true")
	}

	// Update via Upsert
	_, inserted2, err := db.Upsert(12345, &UserWithIntPK{SKU: 12345, Name: "Product A Updated"})
	if err != nil {
		t.Fatalf("failed to upsert update: %v", err)
	}
	if inserted2 {
		t.Error("expected inserted to be false for update")
	}

	// Verify
	found, err := db.Get(12345)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if found.Name != "Product A Updated" {
		t.Errorf("expected Product A Updated, got %s", found.Name)
	}
}

func TestUpsertTable(t *testing.T) {
	dbPath := "test_upsert_table.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[UserWithStringPK](dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	table, err := db.Table()
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	// Insert via Upsert
	_, inserted, err := table.Upsert("alice@example.com", &UserWithStringPK{Email: "alice@example.com", Name: "Alice", Age: 30})
	if err != nil {
		t.Fatalf("failed to upsert: %v", err)
	}
	if !inserted {
		t.Error("expected inserted to be true")
	}

	// Update via Upsert
	_, inserted2, err := table.Upsert("alice@example.com", &UserWithStringPK{Email: "alice@example.com", Name: "Alice Updated"})
	if err != nil {
		t.Fatalf("failed to upsert update: %v", err)
	}
	if inserted2 {
		t.Error("expected inserted to be false")
	}

	// Verify
	found, err := table.Get("alice@example.com")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if found.Name != "Alice Updated" {
		t.Errorf("expected Alice Updated, got %s", found.Name)
	}
}
