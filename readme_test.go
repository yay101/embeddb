package embeddb

import (
	"fmt"
	"os"
	"testing"
)

type User struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Email string `db:"unique,index"`
	Age   int    `db:"index"`
}

type Product struct {
	SKU   int     `db:"id,primary"`
	Name  string  `db:"index"`
	Price float64 `db:"index"`
}

type Order struct {
	ID         uint32 `db:"id,primary"`
	CustomerID uint32 `db:"index"`
	Total      float64
	Status     string `db:"index"`
}

type NestedUser struct {
	ID      uint32 `db:"id,primary"`
	Name    string `db:"index"`
	Address Address
}

type Address struct {
	Street string
	City   string `db:"index"`
	Zip    string
}

func TestReadmeQuickStart(t *testing.T) {
	os.Remove("/tmp/readme_test.db")
	defer os.Remove("/tmp/readme_test.db")

	db, err := Open("/tmp/readme_test.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	id, err := users.Insert(&User{Name: "Alice", Email: "alice@example.com", Age: 30})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Error("expected non-zero ID")
	}

	user, err := users.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if user.Name != "Alice" {
		t.Errorf("expected Alice, got %s", user.Name)
	}

	results, err := users.Query("Email", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	adults, err := users.QueryRangeGreaterThan("Age", 18, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(adults) != 1 {
		t.Errorf("expected 1 adult, got %d", len(adults))
	}
}

func TestIntPrimaryKey(t *testing.T) {
	os.Remove("/tmp/int_pk.db")
	defer os.Remove("/tmp/int_pk.db")

	db, _ := Open("/tmp/int_pk.db")
	defer db.Close()

	products, _ := Use[Product](db, "products")

	_, err := products.Insert(&Product{SKU: 100, Name: "Widget", Price: 9.99})
	if err != nil {
		t.Fatal(err)
	}

	prod, err := products.Get(100)
	if err != nil {
		t.Fatal(err)
	}
	if prod.SKU != 100 {
		t.Errorf("expected SKU 100, got %d", prod.SKU)
	}
}

func TestStringPrimaryKey(t *testing.T) {
	os.Remove("/tmp/string_pk.db")
	defer os.Remove("/tmp/string_pk.db")

	db, _ := Open("/tmp/string_pk.db")
	defer db.Close()

	type UserWithUUID struct {
		UUID  string `db:"id,primary"`
		Name  string
		Email string `db:"index"`
	}

	users, _ := Use[UserWithUUID](db, "users")

	_, err := users.Insert(&UserWithUUID{UUID: "user-123", Name: "Bob", Email: "bob@example.com"})
	if err != nil {
		t.Fatal(err)
	}

	user, err := users.Get("user-123")
	if err != nil {
		t.Fatal(err)
	}
	if user.UUID != "user-123" {
		t.Errorf("expected UUID user-123, got %s", user.UUID)
	}
}

func TestQueryNotEqual(t *testing.T) {
	os.Remove("/tmp/query_ne.db")
	defer os.Remove("/tmp/query_ne.db")

	db, _ := Open("/tmp/query_ne.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 30})
	users.Insert(&User{Name: "Charlie", Age: 25})

	results, err := users.QueryNotEqual("Age", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestQueryGreaterOrEqual(t *testing.T) {
	os.Remove("/tmp/query_ge.db")
	defer os.Remove("/tmp/query_ge.db")

	db, _ := Open("/tmp/query_ge.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 25})
	users.Insert(&User{Name: "Charlie", Age: 35})

	results, err := users.QueryGreaterOrEqual("Age", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestQueryLessOrEqual(t *testing.T) {
	os.Remove("/tmp/query_le.db")
	defer os.Remove("/tmp/query_le.db")

	db, _ := Open("/tmp/query_le.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 25})
	users.Insert(&User{Name: "Charlie", Age: 35})

	results, err := users.QueryLessOrEqual("Age", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestQueryRangeBetween(t *testing.T) {
	os.Remove("/tmp/query_between.db")
	defer os.Remove("/tmp/query_between.db")

	db, _ := Open("/tmp/query_between.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 25})
	users.Insert(&User{Name: "Charlie", Age: 35})
	users.Insert(&User{Name: "Diana", Age: 40})

	results, err := users.QueryRangeBetween("Age", 25, 35, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestQueryLike(t *testing.T) {
	os.Remove("/tmp/query_like.db")
	defer os.Remove("/tmp/query_like.db")

	db, _ := Open("/tmp/query_like.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Smithson", Age: 30})
	users.Insert(&User{Name: "Bob Johnson", Age: 25})
	users.Insert(&User{Name: "Charliesmith", Age: 35})

	results, err := users.QueryLike("Name", "Smith%")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for Smith prefix, got %d", len(results))
	}

	results, err = users.QueryLike("Name", "%smith%")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for contains Smith, got %d", len(results))
	}
}

func TestQueryNotLike(t *testing.T) {
	os.Remove("/tmp/query_notlike.db")
	defer os.Remove("/tmp/query_notlike.db")

	db, _ := Open("/tmp/query_notlike.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Alice", Email: "alice@gmail.com", Age: 30})
	users.Insert(&User{Name: "Bob", Email: "bob@spam.com", Age: 25})
	users.Insert(&User{Name: "Charlie", Email: "charlie@gmail.com", Age: 35})

	results, err := users.QueryNotLike("Email", "%@spam.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestInsertMany(t *testing.T) {
	os.Remove("/tmp/insert_many.db")
	defer os.Remove("/tmp/insert_many.db")

	db, _ := Open("/tmp/insert_many.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	records := []*User{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Charlie", Age: 35},
	}

	ids, err := users.InsertMany(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Errorf("expected 3 IDs, got %d", len(ids))
	}

	count := users.Count()
	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}
}

func TestUpdateMany(t *testing.T) {
	os.Remove("/tmp/update_many.db")
	defer os.Remove("/tmp/update_many.db")

	db, _ := Open("/tmp/update_many.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	id1, _ := users.Insert(&User{Name: "Alice", Age: 30})
	id2, _ := users.Insert(&User{Name: "Bob", Age: 25})
	id3, _ := users.Insert(&User{Name: "Charlie", Age: 35})

	ids := []any{id1, id2, id3}
	updated, err := users.UpdateMany(ids, func(u *User) error {
		u.Age++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated != 3 {
		t.Errorf("expected 3 updated, got %d", updated)
	}

	u1, _ := users.Get(id1)
	if u1.Age != 31 {
		t.Errorf("expected Age 31, got %d", u1.Age)
	}
}

func TestDeleteMany(t *testing.T) {
	os.Remove("/tmp/delete_many.db")
	defer os.Remove("/tmp/delete_many.db")

	db, _ := Open("/tmp/delete_many.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	id1, _ := users.Insert(&User{Name: "Alice", Age: 30})
	id2, _ := users.Insert(&User{Name: "Bob", Age: 25})
	users.Insert(&User{Name: "Charlie", Age: 35})

	ids := []any{id1, id2}
	deleted, err := users.DeleteMany(ids)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}

	count := users.Count()
	if count != 1 {
		t.Errorf("expected 1 remaining, got %d", count)
	}
}

func TestTransactionCommit(t *testing.T) {
	os.Remove("/tmp/tx_commit.db")
	defer os.Remove("/tmp/tx_commit.db")

	db, _ := Open("/tmp/tx_commit.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	_ = db.Begin()
	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 25})

	if err := db.Commit(); err != nil {
		t.Fatal(err)
	}

	count := users.Count()
	if count != 2 {
		t.Errorf("expected 2 records after commit, got %d", count)
	}
}

func TestTransactionRollback(t *testing.T) {
	os.Remove("/tmp/tx_rollback.db")
	defer os.Remove("/tmp/tx_rollback.db")

	db, _ := Open("/tmp/tx_rollback.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	_ = db.Begin()
	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 25})

	if err := db.Rollback(); err != nil {
		t.Fatal(err)
	}

	count := users.Count()
	if count != 0 {
		t.Errorf("expected 0 records after rollback, got %d", count)
	}
}

func TestMultipleTables(t *testing.T) {
	os.Remove("/tmp/multi_table.db")
	defer os.Remove("/tmp/multi_table.db")

	db, _ := Open("/tmp/multi_table.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	orders, _ := Use[Order](db, "orders")

	users.Insert(&User{Name: "Alice", Age: 30})
	orders.Insert(&Order{CustomerID: 1, Total: 99.99, Status: "pending"})

	if users.Count() != 1 {
		t.Errorf("expected 1 user, got %d", users.Count())
	}
	if orders.Count() != 1 {
		t.Errorf("expected 1 order, got %d", orders.Count())
	}
}

func TestNestedStruct(t *testing.T) {
	os.Remove("/tmp/nested.db")
	defer os.Remove("/tmp/nested.db")

	db, _ := Open("/tmp/nested.db")
	defer db.Close()

	users, _ := Use[NestedUser](db, "users")

	users.Insert(&NestedUser{
		Name: "Alice",
		Address: Address{
			Street: "123 Main St",
			City:   "New York",
			Zip:    "10001",
		},
	})

	results, err := users.Query("Address.City", "New York")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestPagination(t *testing.T) {
	os.Remove("/tmp/paged.db")
	defer os.Remove("/tmp/paged.db")

	db, _ := Open("/tmp/paged.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	for i := 0; i < 25; i++ {
		users.Insert(&User{Name: fmt.Sprintf("User%d", i), Age: 25})
	}

	result, err := users.QueryPaged("Age", 25, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 10 {
		t.Errorf("expected 10 records, got %d", len(result.Records))
	}
	if result.TotalCount != 25 {
		t.Errorf("expected TotalCount 25, got %d", result.TotalCount)
	}
	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}
}

func TestScanner(t *testing.T) {
	os.Remove("/tmp/scanner.db")
	defer os.Remove("/tmp/scanner.db")

	db, _ := Open("/tmp/scanner.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	for i := 0; i < 10; i++ {
		users.Insert(&User{Name: fmt.Sprintf("User%d", i), Age: 20 + i})
	}

	count := 0
	scanner := users.ScanRecords()
	for scanner.Next() {
		_, err := scanner.Record()
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	scanner.Close()

	if count != 10 {
		t.Errorf("expected 10 records scanned, got %d", count)
	}
}

func TestAll(t *testing.T) {
	os.Remove("/tmp/all.db")
	defer os.Remove("/tmp/all.db")

	db, _ := Open("/tmp/all.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 25})

	all, err := users.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 records, got %d", len(all))
	}
}

func TestCreateIndex(t *testing.T) {
	os.Remove("/tmp/create_idx.db")
	defer os.Remove("/tmp/create_idx.db")

	db, _ := Open("/tmp/create_idx.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	err := users.CreateIndex("Email")
	if err != nil {
		t.Fatal(err)
	}

	users.Insert(&User{Name: "Alice", Email: "alice@example.com", Age: 30})

	results, err := users.Query("Email", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestDropIndex(t *testing.T) {
	os.Remove("/tmp/drop_idx.db")
	defer os.Remove("/tmp/drop_idx.db")

	db, _ := Open("/tmp/drop_idx.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	users.CreateIndex("Email")
	users.Insert(&User{Name: "Alice", Email: "alice@example.com", Age: 30})

	users.DropIndex("Email")

	results, err := users.Query("Email", "alice@example.com")
	if err == nil {
		t.Error("expected error after dropping index")
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after dropping index, got %d", len(results))
	}
}

func TestUpsert(t *testing.T) {
	os.Remove("/tmp/upsert.db")
	defer os.Remove("/tmp/upsert.db")

	db, _ := Open("/tmp/upsert.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	id, inserted, err := users.Upsert(uint32(1), &User{ID: 1, Name: "Alice", Age: 30})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Error("expected inserted=true for new record")
	}
	if id != 1 {
		t.Errorf("expected ID 1, got %d", id)
	}

	_, inserted, err = users.Upsert(uint32(1), &User{ID: 1, Name: "Alice Updated", Age: 31})
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Error("expected inserted=false for existing record")
	}

	user, _ := users.Get(1)
	if user.Name != "Alice Updated" {
		t.Errorf("expected updated name, got %s", user.Name)
	}
}

func TestFilter(t *testing.T) {
	os.Remove("/tmp/filter.db")
	defer os.Remove("/tmp/filter.db")

	db, _ := Open("/tmp/filter.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	users.Insert(&User{Name: "Alice", Age: 30})
	users.Insert(&User{Name: "Bob", Age: 15})
	users.Insert(&User{Name: "Charlie", Age: 25})

	results, err := users.Filter(func(u User) bool {
		return u.Age >= 18
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 filtered results, got %d", len(results))
	}
}

func TestCount(t *testing.T) {
	os.Remove("/tmp/count.db")
	defer os.Remove("/tmp/count.db")

	db, _ := Open("/tmp/count.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	if users.Count() != 0 {
		t.Errorf("expected 0 count, got %d", users.Count())
	}

	users.Insert(&User{Name: "Alice"})
	users.Insert(&User{Name: "Bob"})

	if users.Count() != 2 {
		t.Errorf("expected 2 count, got %d", users.Count())
	}
}

func TestDrop(t *testing.T) {
	os.Remove("/tmp/drop.db")
	defer os.Remove("/tmp/drop.db")

	db, _ := Open("/tmp/drop.db")
	defer db.Close()

	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Alice"})

	err := users.Drop()
	if err != nil {
		t.Fatal(err)
	}

	if users.Count() != 0 {
		t.Errorf("expected 0 after drop, got %d", users.Count())
	}
}

func TestSync(t *testing.T) {
	os.Remove("/tmp/sync.db")
	defer os.Remove("/tmp/sync.db")

	db, _ := Open("/tmp/sync.db")
	users, _ := Use[User](db, "users")
	users.Insert(&User{Name: "Alice", Age: 30})

	err := db.Sync()
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	db2, _ := Open("/tmp/sync.db")
	defer db2.Close()

	users2, _ := Use[User](db2, "users")
	if users2.Count() != 1 {
		t.Errorf("expected 1 after reopen, got %d", users2.Count())
	}
}

func TestGetIndexedFields(t *testing.T) {
	os.Remove("/tmp/indexed_fields.db")
	defer os.Remove("/tmp/indexed_fields.db")

	db, _ := Open("/tmp/indexed_fields.db")
	defer db.Close()

	users, _ := Use[User](db, "users")

	users.CreateIndex("Email")
	users.CreateIndex("Name")

	fields := users.GetIndexedFields()
	if len(fields) != 2 {
		t.Errorf("expected 2 indexed fields, got %d", len(fields))
	}
}
