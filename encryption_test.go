package embeddb

import (
	"testing"
)

func TestEncryptedField(t *testing.T) {
	type User struct {
		ID    uint32 `db:"id,primary"`
		Name  string `db:"index"`
		Email string `db:"encrypt"`
	}

	key := []byte("01234567890123456789012345678901") // 32-byte AES-256 key

	db, err := Open("/tmp/test_encrypted_field.db", OpenOptions{AutoIndex: false, EncryptionKey: key})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	users, err := Use[User](db, "users")
	if err != nil {
		t.Fatalf("Use: %v", err)
	}

	id, err := users.Insert(&User{Name: "Alice", Email: "alice@example.com"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	user, err := users.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if user.Name != "Alice" {
		t.Errorf("Name mismatch: got %q want Alice", user.Name)
	}
	if user.Email != "alice@example.com" {
		t.Errorf("Email mismatch: got %q want alice@example.com", user.Email)
	}

	// Update
	if err := users.Update(id, &User{Name: "Alice Updated", Email: "alice2@example.com"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	user, err = users.Get(id)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if user.Email != "alice2@example.com" {
		t.Errorf("Email after update mismatch: got %q", user.Email)
	}

	// Re-open with same key
	db.Close()
	db, err = Open("/tmp/test_encrypted_field.db", OpenOptions{AutoIndex: false, EncryptionKey: key})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db.Close()
	users, err = Use[User](db, "users")
	if err != nil {
		t.Fatalf("Use after reopen: %v", err)
	}
	user, err = users.Get(id)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if user.Email != "alice2@example.com" {
		t.Errorf("Email after reopen mismatch: got %q", user.Email)
	}
}

func TestEncryptedFieldWrongKey(t *testing.T) {
	type User struct {
		ID    uint32 `db:"id,primary"`
		Name  string `db:"index"`
		Email string `db:"encrypt"`
	}

	key := []byte("01234567890123456789012345678901")
	db, err := Open("/tmp/test_enc_wrong_key.db", OpenOptions{AutoIndex: false, EncryptionKey: key})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	users, err := Use[User](db, "users")
	if err != nil {
		t.Fatalf("Use: %v", err)
	}
	id, err := users.Insert(&User{Name: "Bob", Email: "bob@example.com"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	db.Close()

	// Re-open with wrong key - Email should decrypt to empty because GCM tag fails
	wrongKey := []byte("00000000000000000000000000000000")
	db2, err := Open("/tmp/test_enc_wrong_key.db", OpenOptions{AutoIndex: false, EncryptionKey: wrongKey})
	if err != nil {
		t.Fatalf("Open with wrong key: %v", err)
	}
	defer db2.Close()
	users2, err := Use[User](db2, "users")
	if err != nil {
		t.Fatalf("Use: %v", err)
	}

	// Since decryption fails, Email field should remain zero value, but record should still be readable
	user, err := users2.Get(id)
	if err != nil {
		t.Fatalf("Get with wrong key: %v", err)
	}
	if user.Name != "Bob" {
		t.Errorf("Name should still be readable, got %q", user.Name)
	}
	if user.Email != "" {
		t.Errorf("Email should be empty due to decryption failure, got %q", user.Email)
	}
}

func TestEncryptedFieldWithIndexRejected(t *testing.T) {
	type User struct {
		ID    uint32 `db:"id,primary"`
		Email string `db:"index,encrypt"`
	}

	key := []byte("01234567890123456789012345678901")
	db, err := Open("/tmp/test_enc_idx_rejected.db", OpenOptions{AutoIndex: true, EncryptionKey: key})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	users, err := Use[User](db, "users")
	if err != nil {
		t.Fatalf("Use: %v", err)
	}

	id, err := users.Insert(&User{Email: "alice@example.com"})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	user, err := users.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if user.Email != "" {
		// Email was skipped because index+encrypt is rejected during layout computation
		t.Errorf("Email should be empty for index+encrypt field, got %q", user.Email)
	}
}

func TestEncryptedFieldInSliceOfStructs(t *testing.T) {
	type Item struct {
		Name  string  `db:""`
		Price float64 `db:"encrypt"`
	}

	type Cart struct {
		ID    uint32 `db:"id,primary"`
		Items []Item `db:""`
	}

	key := []byte("01234567890123456789012345678901")
	db, err := Open("/tmp/test_enc_slice_struct.db", OpenOptions{AutoIndex: false, EncryptionKey: key})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	carts, err := Use[Cart](db, "carts")
	if err != nil {
		t.Fatalf("Use: %v", err)
	}

	id, err := carts.Insert(&Cart{
		Items: []Item{
			{Name: "Widget", Price: 9.99},
			{Name: "Gadget", Price: 19.99},
		},
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	cart, err := carts.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(cart.Items) != 2 {
		t.Fatalf("Items length mismatch: got %d want 2", len(cart.Items))
	}
	if cart.Items[0].Name != "Widget" {
		t.Errorf("Item 0 name mismatch: got %q", cart.Items[0].Name)
	}
	if cart.Items[0].Price != 9.99 {
		t.Errorf("Item 0 price mismatch: got %f", cart.Items[0].Price)
	}
	if cart.Items[1].Name != "Gadget" {
		t.Errorf("Item 1 name mismatch: got %q", cart.Items[1].Name)
	}
	if cart.Items[1].Price != 19.99 {
		t.Errorf("Item 1 price mismatch: got %f", cart.Items[1].Price)
	}
}
