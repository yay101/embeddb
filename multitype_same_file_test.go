package embeddb

import (
	"fmt"
	"os"
	"testing"
)

type multiTypeUser struct {
	ID   uint32 `db:"id,primary"`
	Name string `db:"index"`
}

type multiTypeOrder struct {
	ID    uint32 `db:"id,primary"`
	Item  string `db:"index"`
	Price int
}

func TestMultiTypeSameFileConcurrentHandles(t *testing.T) {
	fn := "/tmp/embeddb_multitype_same_file.db"
	_ = os.Remove(fn)
	defer os.Remove(fn)

	userDB, err := New[multiTypeUser](fn, false, false)
	if err != nil {
		t.Fatal(err)
	}

	orderDB, err := New[multiTypeOrder](fn, false, false)
	if err != nil {
		t.Fatal(err)
	}

	users, err := userDB.Table("users")
	if err != nil {
		t.Fatal(err)
	}

	orders, err := orderDB.Table("orders")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		if _, err := users.Insert(&multiTypeUser{Name: fmt.Sprintf("u-%d", i)}); err != nil {
			t.Fatalf("insert user %d failed: %v", i, err)
		}
		if _, err := orders.Insert(&multiTypeOrder{Item: fmt.Sprintf("o-%d", i), Price: i}); err != nil {
			t.Fatalf("insert order %d failed: %v", i, err)
		}
	}

	if err := userDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := orderDB.Close(); err != nil {
		t.Fatal(err)
	}

	userDB2, err := New[multiTypeUser](fn, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer userDB2.Close()

	orderDB2, err := New[multiTypeOrder](fn, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer orderDB2.Close()

	users2, err := userDB2.Table("users")
	if err != nil {
		t.Fatal(err)
	}
	orders2, err := orderDB2.Table("orders")
	if err != nil {
		t.Fatal(err)
	}

	if got := users2.Count(); got != 50 {
		t.Fatalf("expected 50 users, got %d", got)
	}
	if got := orders2.Count(); got != 50 {
		t.Fatalf("expected 50 orders, got %d", got)
	}
}
