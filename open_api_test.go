package embeddb

import (
	"fmt"
	"os"
	"testing"
)

type openAPIUser struct {
	ID   uint32 `db:"id,primary"`
	Name string
}

type openAPIOrder struct {
	ID    uint32 `db:"id,primary"`
	Item  string
	Price int
}

func TestOpenUseMultiType(t *testing.T) {
	fn := "/tmp/embeddb_open_use_multitype.db"
	_ = os.Remove(fn)
	defer os.Remove(fn)

	db, err := Open(fn)
	if err != nil {
		t.Fatal(err)
	}

	users, err := Use[openAPIUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}
	orders, err := Use[openAPIOrder](db, "orders")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if _, err := users.Insert(&openAPIUser{Name: fmt.Sprintf("u-%d", i)}); err != nil {
			t.Fatalf("insert user %d failed: %v", i, err)
		}
		if _, err := orders.Insert(&openAPIOrder{Item: fmt.Sprintf("o-%d", i), Price: i}); err != nil {
			t.Fatalf("insert order %d failed: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(fn)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	users2, err := Use[openAPIUser](db2, "users")
	if err != nil {
		t.Fatal(err)
	}
	orders2, err := Use[openAPIOrder](db2, "orders")
	if err != nil {
		t.Fatal(err)
	}

	if got := users2.Count(); got != 10 {
		t.Fatalf("expected 10 users, got %d", got)
	}
	if got := orders2.Count(); got != 10 {
		t.Fatalf("expected 10 orders, got %d", got)
	}
}

func TestUseReturnsCachedTableHandle(t *testing.T) {
	fn := "/tmp/embeddb_open_use_cached.db"
	_ = os.Remove(fn)
	defer os.Remove(fn)

	db, err := Open(fn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t1, err := Use[openAPIUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}
	t2, err := Use[openAPIUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	if t1 != t2 {
		t.Fatal("expected Use to return cached table handle")
	}
}
