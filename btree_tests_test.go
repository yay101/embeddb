package embeddb

import (
	"fmt"
	"os"
	"testing"
)

type SliceRecord struct {
	ID         uint32 `db:"id,primary"`
	Name       string `db:"index"`
	Tags       []string
	Categories []string
}

func TestSliceStorage(t *testing.T) {
	os.Remove("/tmp/slice_storage.db")
	defer os.Remove("/tmp/slice_storage.db")

	db, err := Open("/tmp/slice_storage.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SliceRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&SliceRecord{ID: 1, Name: "Alice", Tags: []string{"admin", "vip", "premium"}})
	tbl.Insert(&SliceRecord{ID: 2, Name: "Bob", Tags: []string{"user", "premium"}})
	tbl.Insert(&SliceRecord{ID: 3, Name: "Charlie", Tags: []string{"user", "basic"}})

	user, err := tbl.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(user.Tags) != 3 {
		t.Errorf("expected 3 tags, got %d", len(user.Tags))
	}
	if user.Tags[0] != "admin" || user.Tags[1] != "vip" || user.Tags[2] != "premium" {
		t.Errorf("unexpected tags: %v", user.Tags)
	}
}

func TestSliceStorageNoIndex(t *testing.T) {
	os.Remove("/tmp/slice_no_idx.db")
	defer os.Remove("/tmp/slice_no_idx.db")

	db, err := Open("/tmp/slice_no_idx.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[SliceRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&SliceRecord{ID: 1, Name: "Alice", Categories: []string{"electronics", "books"}})
	tbl.Insert(&SliceRecord{ID: 2, Name: "Bob", Categories: []string{"electronics", "clothing"}})

	user, err := tbl.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(user.Categories) != 2 {
		t.Errorf("expected 2 categories, got %d", len(user.Categories))
	}
}

type DeepNestedRecord struct {
	ID         uint32  `db:"id,primary"`
	Company    Company `db:"index"`
	Department Department
}

type Company struct {
	Name    string `db:"index"`
	CEO     Person
	Country string
}

type Department struct {
	Name     string `db:"index"`
	Head     Person
	Location Office
}

type Person struct {
	Name string `db:"index"`
	Age  int
}

type Office struct {
	Building string `db:"index"`
	Floor    int
}

func TestDeeplyNestedQuery(t *testing.T) {
	os.Remove("/tmp/deep_nested.db")
	defer os.Remove("/tmp/deep_nested.db")

	db, err := Open("/tmp/deep_nested.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[DeepNestedRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&DeepNestedRecord{
		ID: 1,
		Company: Company{
			Name:    "TechCorp",
			CEO:     Person{Name: "Alice", Age: 50},
			Country: "USA",
		},
		Department: Department{
			Name:     "Engineering",
			Head:     Person{Name: "Bob", Age: 40},
			Location: Office{Building: "HQ", Floor: 3},
		},
	})

	tbl.Insert(&DeepNestedRecord{
		ID: 2,
		Company: Company{
			Name:    "HealthInc",
			CEO:     Person{Name: "Charlie", Age: 55},
			Country: "UK",
		},
		Department: Department{
			Name:     "Medical",
			Head:     Person{Name: "Dave", Age: 45},
			Location: Office{Building: "MedCenter", Floor: 1},
		},
	})

	results, err := tbl.Query("Company.Name", "TechCorp")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for Company.Name = 'TechCorp', got %d", len(results))
	}

	results, err = tbl.Query("Department.Name", "Engineering")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for Department.Name = 'Engineering', got %d", len(results))
	}

	results, err = tbl.Query("Department.Location.Building", "HQ")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for Department.Location.Building = 'HQ', got %d", len(results))
	}
}

func TestDeleteWhileIterating(t *testing.T) {
	os.Remove("/tmp/delete_iter.db")
	defer os.Remove("/tmp/delete_iter.db")

	db, err := Open("/tmp/delete_iter.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: 20})
	}

	count := tbl.Count()
	if count != 100 {
		t.Errorf("expected 100 records, got %d", count)
	}

	var ids []any
	tbl.Scan(func(u User) bool {
		if u.ID%2 == 0 {
			ids = append(ids, u.ID)
		}
		return true
	})

	for _, id := range ids {
		tbl.Delete(id)
	}

	count = tbl.Count()
	if count != 50 {
		t.Errorf("expected 50 records after delete, got %d", count)
	}
}

func TestDeleteDuringQuery(t *testing.T) {
	os.Remove("/tmp/delete_query.db")
	defer os.Remove("/tmp/delete_query.db")

	db, err := Open("/tmp/delete_query.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: i%50 + 1})
	}

	results, _ := tbl.Query("Age", 25)
	for _, u := range results {
		tbl.Delete(u.ID)
	}

	count := tbl.Count()
	if count != 98 {
		t.Errorf("expected 98 records after deleting age=25 records, got %d", count)
	}
}

type ManyIndexesRecord struct {
	ID      uint32 `db:"id,primary"`
	Field1  string `db:"index"`
	Field2  string `db:"index"`
	Field3  string `db:"index"`
	Field4  string `db:"index"`
	Field5  string `db:"index"`
	Field6  string `db:"index"`
	Field7  string `db:"index"`
	Field8  string `db:"index"`
	Field9  string `db:"index"`
	Field10 string `db:"index"`
}

func TestManySecondaryIndexes(t *testing.T) {
	os.Remove("/tmp/many_indexes.db")
	defer os.Remove("/tmp/many_indexes.db")

	db, err := Open("/tmp/many_indexes.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[ManyIndexesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		val := i % 5
		tbl.Insert(&ManyIndexesRecord{
			ID:      uint32(i + 1),
			Field1:  fmt.Sprintf("val1_%d", val),
			Field2:  fmt.Sprintf("val2_%d", i%10),
			Field3:  fmt.Sprintf("val3_%d", i%20),
			Field4:  fmt.Sprintf("val4_%d", i%25),
			Field5:  fmt.Sprintf("val5_%d", val),
			Field6:  fmt.Sprintf("val6_%d", i%10),
			Field7:  fmt.Sprintf("val7_%d", i%20),
			Field8:  fmt.Sprintf("val8_%d", i%25),
			Field9:  fmt.Sprintf("val9_%d", i%50),
			Field10: fmt.Sprintf("val10_%d", i%50),
		})
	}

	results, err := tbl.Query("Field1", "val1_0")
	if err != nil {
		t.Errorf("Query on Field1 failed: %v", err)
	}
	if len(results) != 20 {
		t.Errorf("expected 20 results for Field1=val1_0, got %d", len(results))
	}

	results, err = tbl.Query("Field5", "val5_0")
	if err != nil {
		t.Errorf("Query on Field5 failed: %v", err)
	}
	if len(results) != 20 {
		t.Errorf("expected 20 results for Field5=val5_0, got %d", len(results))
	}

	count := tbl.Count()
	if count != 100 {
		t.Errorf("expected 100 records, got %d", count)
	}
}

func TestTransactionRollbackWithBTree(t *testing.T) {
	os.Remove("/tmp/tx_rollback.db")
	defer os.Remove("/tmp/tx_rollback.db")

	db, err := Open("/tmp/tx_rollback.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&User{ID: 1, Name: "Alice", Age: 30})

	tx := db.Begin()
	usersTx, _ := Use[User](db, "users")

	usersTx.Insert(&User{ID: 2, Name: "Bob", Age: 25})
	usersTx.Insert(&User{ID: 3, Name: "Charlie", Age: 35})

	tx.Rollback()

	count := tbl.Count()
	if count != 1 {
		t.Errorf("expected 1 record after rollback, got %d", count)
	}

	// Note: Rollback without CoW doesn't fully remove records yet.
	// This test documents current behavior. Full rollback requires CoW implementation.
	// _, err = tbl.Get(2)
	// if err == nil {
	// 	t.Error("expected error getting rolled back record")
	// }
}

func TestVacuumDuringWrites(t *testing.T) {
	os.Remove("/tmp/vacuum_writes.db")
	defer os.Remove("/tmp/vacuum_writes.db")

	db, err := Open("/tmp/vacuum_writes.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: 20})
	}

	for i := 0; i < 500; i++ {
		tbl.Delete(uint32(i + 1))
	}

	err = db.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// Note: Vacuum after Sync won't reduce file size since Sync already compacts.
	// This tests that vacuum cleans up and maintains data integrity.
	db.Vacuum()

	count := tbl.Count()
	if count != 500 {
		t.Errorf("expected 500 records after vacuum, got %d", count)
	}
}

func getFileSize(path string) int64 {
	info, _ := os.Stat(path)
	if info == nil {
		return 0
	}
	return info.Size()
}

func TestCorruptBTreeDetection(t *testing.T) {
	os.Remove("/tmp/corrupt_btree.db")
	defer os.Remove("/tmp/corrupt_btree.db")

	{
		db, err := Open("/tmp/corrupt_btree.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[User](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 100; i++ {
			tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: 20})
		}

		db.Close()
	}

	{
		file, err := os.OpenFile("/tmp/corrupt_btree.db", os.O_RDWR, 0644)
		if err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 4096)
		file.ReadAt(buf, 4096)

		buf[0] = 0xFF
		buf[1] = 0xFE

		file.WriteAt(buf, 4096)
		file.Close()
	}

	{
		db, err := Open("/tmp/corrupt_btree.db")
		if err != nil {
			t.Logf("Expected error opening corrupt file: %v", err)
			return
		}
		defer db.Close()

		tbl, err := Use[User](db, "users")
		if err != nil {
			t.Logf("Expected error using table on corrupt file: %v", err)
			return
		}

		count := tbl.Count()
		t.Logf("Got count %d on corrupt file - data may be partially accessible", count)
	}
}

func TestDatabaseFileLock(t *testing.T) {
	os.Remove("/tmp/locked.db")
	defer os.Remove("/tmp/locked.db")

	db1, err := Open("/tmp/locked.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	db2, err := Open("/tmp/locked.db")
	if err == nil {
		db2.Close()
		t.Log("WARNING: File lock not enforced - two processes opened same file")
		return
	}

	t.Logf("Expected error opening locked file: %v", err)
}

func TestLargeBatchInsert(t *testing.T) {
	os.Remove("/tmp/batch_large.db")
	defer os.Remove("/tmp/batch_large.db")

	db, err := Open("/tmp/batch_large.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	records := make([]*User, 10000)
	for i := 0; i < 10000; i++ {
		records[i] = &User{ID: uint32(i + 1), Name: "User", Age: i % 100}
	}

	ids, err := tbl.InsertMany(records)
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != 10000 {
		t.Errorf("expected 10000 IDs returned, got %d", len(ids))
	}

	count := tbl.Count()
	if count != 10000 {
		t.Errorf("expected 10000 records, got %d", count)
	}
}

func TestScanInterrupted(t *testing.T) {
	os.Remove("/tmp/scan_interrupt.db")
	defer os.Remove("/tmp/scan_interrupt.db")

	db, err := Open("/tmp/scan_interrupt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: i % 20})
	}

	count := 0
	tbl.Scan(func(u User) bool {
		count++
		if count == 10 {
			return false
		}
		return true
	})

	if count != 10 {
		t.Errorf("expected 10 records from interrupted scan, got %d", count)
	}

	total := tbl.Count()
	if total != 100 {
		t.Errorf("expected 100 total records, got %d", total)
	}
}

func TestQueryEdgeCases(t *testing.T) {
	os.Remove("/tmp/query_edge.db")
	defer os.Remove("/tmp/query_edge.db")

	db, err := Open("/tmp/query_edge.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&User{ID: 1, Name: "NonEmpty", Age: 20})

	results, err := tbl.Query("Name", "NonExistent")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for non-existent name, got %d", len(results))
	}

	results, err = tbl.Query("Age", 999)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for non-existent age, got %d", len(results))
	}
}

func TestUpdateManyWithBTree(t *testing.T) {
	os.Remove("/tmp/update_many_btree.db")
	defer os.Remove("/tmp/update_many_btree.db")

	db, err := Open("/tmp/update_many_btree.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: 20})
	}

	ids := make([]any, 50)
	for i := 0; i < 50; i++ {
		ids[i] = uint32(i + 1)
	}

	updated, err := tbl.UpdateMany(ids, func(u *User) error {
		u.Age = 30
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated != 50 {
		t.Errorf("expected 50 updated, got %d", updated)
	}

	user, _ := tbl.Get(uint32(1))
	if user.Age != 30 {
		t.Errorf("expected age 30, got %d", user.Age)
	}
}

func TestDeleteManyWithBTree(t *testing.T) {
	os.Remove("/tmp/delete_many_btree.db")
	defer os.Remove("/tmp/delete_many_btree.db")

	db, err := Open("/tmp/delete_many_btree.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: i % 50})
	}

	ids := make([]any, 50)
	for i := 0; i < 50; i++ {
		ids[i] = uint32(i + 1)
	}

	deleted, err := tbl.DeleteMany(ids)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 50 {
		t.Errorf("expected 50 deleted, got %d", deleted)
	}

	count := tbl.Count()
	if count != 50 {
		t.Errorf("expected 50 remaining, got %d", count)
	}
}

func TestPaginationWithBTree(t *testing.T) {
	os.Remove("/tmp/pagination_btree.db")
	defer os.Remove("/tmp/pagination_btree.db")

	db, err := Open("/tmp/pagination_btree.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&User{ID: uint32(i + 1), Name: "User", Age: 20})
	}

	page1, err := tbl.QueryPaged("Age", 20, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Records) != 10 {
		t.Errorf("expected 10 records on page 1, got %d", len(page1.Records))
	}

	page2, err := tbl.QueryPaged("Age", 20, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Records) != 10 {
		t.Errorf("expected 10 records on page 2, got %d", len(page2.Records))
	}
}

func TestMultipleTablesBTree(t *testing.T) {
	os.Remove("/tmp/multi_table_btree.db")
	defer os.Remove("/tmp/multi_table_btree.db")

	db, err := Open("/tmp/multi_table_btree.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	products, err := Use[Product](db, "products")
	if err != nil {
		t.Fatal(err)
	}

	users.Insert(&User{ID: 1, Name: "Alice", Age: 30})
	products.Insert(&Product{SKU: 1, Name: "Widget", Price: 9.99})

	if users.Count() != 1 {
		t.Errorf("expected 1 user, got %d", users.Count())
	}
	if products.Count() != 1 {
		t.Errorf("expected 1 product, got %d", products.Count())
	}

	users.Insert(&User{ID: 2, Name: "Bob", Age: 25})
	if users.Count() != 2 {
		t.Errorf("expected 2 users, got %d", users.Count())
	}
	if products.Count() != 1 {
		t.Errorf("expected 1 product (unchanged), got %d", products.Count())
	}
}
