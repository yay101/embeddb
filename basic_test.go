package embeddb

import (
	"os"
	"testing"
	"time"
)

type TestRecord struct {
	ID   uint32 `db:"id,primary"`
	Name string
	Data string
}

func TestBasicWriteRead(t *testing.T) {
	os.Remove("/tmp/test_basic.db")

	db, err := Open("/tmp/test_basic.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	id, err := tbl.Insert(&TestRecord{Name: "hello world"})
	if err != nil {
		t.Fatal(err)
	}

	if id != 1 {
		t.Errorf("expected id 1, got %d", id)
	}

	record, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if record.Name != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", record.Name)
	}

	db.Close()
	os.Remove("/tmp/test_basic.db")
}

func TestBasicMultipleTables(t *testing.T) {
	os.Remove("/tmp/test_multi.db")

	db, err := Open("/tmp/test_multi.db")
	if err != nil {
		t.Fatal(err)
	}

	users, err := Use[TestRecord](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	posts, err := Use[TestRecord](db, "posts")
	if err != nil {
		t.Fatal(err)
	}

	id1, err := users.Insert(&TestRecord{Name: "user1 data"})
	if err != nil {
		t.Fatal(err)
	}

	id2, err := posts.Insert(&TestRecord{Name: "post1 data"})
	if err != nil {
		t.Fatal(err)
	}

	if id1 != 1 || id2 != 1 {
		t.Errorf("expected both ids to be 1, got %d and %d", id1, id2)
	}

	rec1, err := users.Get(id1)
	if err != nil {
		t.Fatal(err)
	}

	rec2, err := posts.Get(id2)
	if err != nil {
		t.Fatal(err)
	}

	if rec1.Name != "user1 data" || rec2.Name != "post1 data" {
		t.Errorf("data mismatch: got '%s' and '%s'", rec1.Name, rec2.Name)
	}

	db.Close()
	os.Remove("/tmp/test_multi.db")
}

func TestMultipleRecords(t *testing.T) {
	os.Remove("/tmp/test_multi_rec.db")

	db, err := Open("/tmp/test_multi_rec.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		_, err := tbl.Insert(&TestRecord{Name: "record"})
		if err != nil {
			t.Fatal(err)
		}
	}

	count := tbl.Count()
	if count != 100 {
		t.Errorf("expected 100 records, got %d", count)
	}

	db.Close()
	os.Remove("/tmp/test_multi_rec.db")
}

func TestUpdate(t *testing.T) {
	os.Remove("/tmp/test_update.db")

	db, err := Open("/tmp/test_update.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	id, _ := tbl.Insert(&TestRecord{Name: "original"})

	err = tbl.Update(id, &TestRecord{ID: id, Name: "updated"})
	if err != nil {
		t.Fatal(err)
	}

	record, _ := tbl.Get(id)
	if record.Name != "updated" {
		t.Errorf("expected 'updated', got '%s'", record.Name)
	}

	db.Close()
	os.Remove("/tmp/test_update.db")
}

func TestDelete(t *testing.T) {
	os.Remove("/tmp/test_delete.db")

	db, err := Open("/tmp/test_delete.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	id, _ := tbl.Insert(&TestRecord{Name: "todelete"})

	err = tbl.Delete(id)
	if err != nil {
		t.Fatal(err)
	}

	count := tbl.Count()
	if count != 0 {
		t.Errorf("expected 0 records after delete, got %d", count)
	}

	db.Close()
	os.Remove("/tmp/test_delete.db")
}

func TestMapIndex(t *testing.T) {
	mi := newMapIndex()

	mi.Set([]byte("key1"), []byte("value1"))
	mi.Set([]byte("key2"), []byte("value2"))

	val, ok := mi.Get([]byte("key1"))
	if !ok {
		t.Error("expected to find key1")
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got '%s'", string(val))
	}

	count := 0
	mi.Range(func(k []byte, v []byte) bool {
		count++
		return true
	})
	if count != 2 {
		t.Errorf("expected 2 entries, got %d", count)
	}

	mi.Delete([]byte("key1"))
	_, ok = mi.Get([]byte("key1"))
	if ok {
		t.Error("expected key1 to be deleted")
	}
}

func TestUint32MapIndex(t *testing.T) {
	mi := newUint32MapIndex()

	mi.Set("key1", 100)
	mi.Set("key2", 200)

	val, ok := mi.Get("key1")
	if !ok {
		t.Error("expected to find key1")
	}
	if val != 100 {
		t.Errorf("expected 100, got %d", val)
	}

	count := 0
	mi.Range(func(k string, v uint32) bool {
		count++
		return true
	})
	if count != 2 {
		t.Errorf("expected 2 entries, got %d", count)
	}

	mi.Delete("key1")
	_, ok = mi.Get("key1")
	if ok {
		t.Error("expected key1 to be deleted")
	}
}

func TestRestartBasic(t *testing.T) {
	os.Remove("/tmp/test_restart.db")

	db, err := Open("/tmp/test_restart.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err := tbl.Insert(&TestRecord{ID: uint32(i + 1), Name: "record", Data: "data"})
		if err != nil {
			t.Fatal(err)
		}
	}

	count := tbl.Count()
	if count != 10 {
		t.Fatalf("expected 10 records before close, got %d", count)
	}

	db.Close()

	db2, err := Open("/tmp/test_restart.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl2, err := Use[TestRecord](db2, "test")
	if err != nil {
		t.Fatal(err)
	}

	count = tbl2.Count()
	if count != 10 {
		t.Fatalf("expected 10 records after restart, got %d", count)
	}

	rec, err := tbl2.Get(uint32(5))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name != "record" || rec.Data != "data" {
		t.Errorf("record data mismatch: got %s/%s", rec.Name, rec.Data)
	}

	db2.Close()
	os.Remove("/tmp/test_restart.db")
}

func TestRestartMultipleTables(t *testing.T) {
	os.Remove("/tmp/test_restart_multi.db")

	db, err := Open("/tmp/test_restart_multi.db")
	if err != nil {
		t.Fatal(err)
	}

	users, err := Use[TestRecord](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	posts, err := Use[TestRecord](db, "posts")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		users.Insert(&TestRecord{Name: "user"})
		posts.Insert(&TestRecord{Data: "post"})
	}

	db.Close()

	db2, err := Open("/tmp/test_restart_multi.db")
	if err != nil {
		t.Fatal(err)
	}

	users2, err := Use[TestRecord](db2, "users")
	if err != nil {
		t.Fatal(err)
	}

	posts2, err := Use[TestRecord](db2, "posts")
	if err != nil {
		t.Fatal(err)
	}

	if users2.Count() != 5 {
		t.Errorf("expected 5 users, got %d", users2.Count())
	}
	if posts2.Count() != 5 {
		t.Errorf("expected 5 posts, got %d", posts2.Count())
	}

	db2.Close()
	os.Remove("/tmp/test_restart_multi.db")
}

func TestRestartCorruptIndex(t *testing.T) {
	os.Remove("/tmp/test_corrupt.db")

	db, err := Open("/tmp/test_corrupt.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[TestRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		_, err := tbl.Insert(&TestRecord{ID: uint32(i + 1), Name: "record", Data: "data"})
		if err != nil {
			t.Fatal(err)
		}
	}

	count := tbl.Count()
	if count != 20 {
		t.Fatalf("expected 20 records before corruption, got %d", count)
	}

	db.Close()

	file, err := os.OpenFile("/tmp/test_corrupt.db", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	file.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, 40)

	file.Close()

	db3, err := Open("/tmp/test_corrupt.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl3, err := Use[TestRecord](db3, "test")
	if err != nil {
		t.Fatal(err)
	}

	count = tbl3.Count()
	if count != 20 {
		t.Fatalf("expected 20 records after index rebuild, got %d", count)
	}

	rec, err := tbl3.Get(uint32(15))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name != "record" || rec.Data != "data" {
		t.Errorf("record data mismatch: got %s/%s", rec.Name, rec.Data)
	}

	all, err := tbl3.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 20 {
		t.Errorf("expected 20 records in All(), got %d", len(all))
	}

	db3.Close()
	os.Remove("/tmp/test_corrupt.db")
}

func TestRestartCorruptMultipleTables(t *testing.T) {
	os.Remove("/tmp/test_corrupt_multi.db")

	db, err := Open("/tmp/test_corrupt_multi.db")
	if err != nil {
		t.Fatal(err)
	}

	users, err := Use[TestRecord](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	posts, err := Use[TestRecord](db, "posts")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		users.Insert(&TestRecord{Name: "user"})
		posts.Insert(&TestRecord{Data: "post"})
	}

	db.Close()

	file, err := os.OpenFile("/tmp/test_corrupt_multi.db", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	file.WriteAt([]byte{0x00}, 40)
	file.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 44)

	file.Close()

	db2, err := Open("/tmp/test_corrupt_multi.db")
	if err != nil {
		t.Fatal(err)
	}

	users2, err := Use[TestRecord](db2, "users")
	if err != nil {
		t.Fatal(err)
	}

	posts2, err := Use[TestRecord](db2, "posts")
	if err != nil {
		t.Fatal(err)
	}

	if users2.Count() != 10 {
		t.Errorf("expected 10 users, got %d", users2.Count())
	}
	if posts2.Count() != 10 {
		t.Errorf("expected 10 posts, got %d", posts2.Count())
	}

	db2.Close()
	os.Remove("/tmp/test_corrupt_multi.db")
}

func TestAutoIndexRangeQuery(t *testing.T) {
	os.Remove("/tmp/test_autoindex.db")
	defer os.Remove("/tmp/test_autoindex.db")

	db, err := Open("/tmp/test_autoindex.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users, err := Use[TestRecord](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	users.Insert(&TestRecord{Name: "Alice", Data: "data1"})
	users.Insert(&TestRecord{Name: "Bob", Data: "data2"})
	users.Insert(&TestRecord{Name: "Charlie", Data: "data3"})

	results, err := users.QueryRangeGreaterThan("Name", "Bob", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	results, err = users.QueryRangeBetween("Name", "Alice", "Charlie", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestAutoIndexRestart(t *testing.T) {
	os.Remove("/tmp/test_autoindex_restart.db")
	defer os.Remove("/tmp/test_autoindex_restart.db")

	{
		db, err := Open("/tmp/test_autoindex_restart.db", OpenOptions{AutoIndex: true})
		if err != nil {
			t.Fatal(err)
		}

		users, err := Use[TestRecord](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		users.Insert(&TestRecord{Name: "Alice", Data: "data1"})
		users.Insert(&TestRecord{Name: "Bob", Data: "data2"})
		users.Insert(&TestRecord{Name: "Charlie", Data: "data3"})

		db.Close()
	}

	{
		db2, err := Open("/tmp/test_autoindex_restart.db", OpenOptions{AutoIndex: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()

		users2, err := Use[TestRecord](db2, "users")
		if err != nil {
			t.Fatal(err)
		}

		results, err := users2.QueryRangeGreaterThan("Name", "Bob", false)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result after restart, got %d", len(results))
		}

		results, err = users2.QueryRangeBetween("Name", "Alice", "Charlie", true, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results after restart, got %d", len(results))
		}
	}
}

func TestMigrateOption(t *testing.T) {
	os.Remove("/tmp/test_migrate.db")
	defer os.Remove("/tmp/test_migrate.db")

	type UserV1 struct {
		ID   uint32 `db:"id,primary"`
		Name string
		Age  int
	}

	type UserV2 struct {
		ID   uint32 `db:"id,primary"`
		Name string
		Age  int
		City string
	}

	{
		db, err := Open("/tmp/test_migrate.db")
		if err != nil {
			t.Fatal(err)
		}

		users, err := Use[UserV1](db, "users")
		if err != nil {
			t.Fatal(err)
		}

		users.Insert(&UserV1{Name: "Alice", Age: 30})
		users.Insert(&UserV1{Name: "Bob", Age: 25})

		count := users.Count()
		if count != 2 {
			t.Errorf("expected 2 records, got %d", count)
		}

		db.Close()
	}

	{
		db2, err := Open("/tmp/test_migrate.db", OpenOptions{Migrate: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()

		users2, err := Use[UserV2](db2, "users")
		if err != nil {
			t.Fatal(err)
		}

		count := users2.Count()
		if count != 2 {
			t.Errorf("expected 2 records after migration, got %d", count)
		}

		rec, err := users2.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if rec.Name != "Alice" || rec.Age != 30 {
			t.Errorf("record data mismatch: got %s/%d", rec.Name, rec.Age)
		}
	}
}

func TestLargeStrings(t *testing.T) {
	os.Remove("/tmp/test_large_string.db")

	db, err := Open("/tmp/test_large_string.db")
	if err != nil {
		t.Fatal(err)
	}

	type LargeRecord struct {
		ID          uint32 `db:"id,primary"`
		Title       string
		Description string
		Data        string
	}

	tbl, err := Use[LargeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}

	longDesc := makeString(5000)
	longData := makeString(20000)

	id, err := tbl.Insert(&LargeRecord{
		Title:       "Test with large strings",
		Description: longDesc,
		Data:        longData,
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if rec.Title != "Test with large strings" {
		t.Errorf("title mismatch: got '%s'", rec.Title)
	}
	if len(rec.Description) != 5000 {
		t.Errorf("description length mismatch: got %d, expected 5000", len(rec.Description))
	}
	if len(rec.Data) != 20000 {
		t.Errorf("data length mismatch: got %d, expected 20000", len(rec.Data))
	}

	db.Close()
	os.Remove("/tmp/test_large_string.db")
}

func TestLargeStringsInNestedStruct(t *testing.T) {
	os.Remove("/tmp/test_nested_large.db")

	db, err := Open("/tmp/test_nested_large.db")
	if err != nil {
		t.Fatal(err)
	}

	type Contact struct {
		Name  string
		Email string
		Notes string
	}

	type Alert struct {
		ID          uint32 `db:"id,primary"`
		TenantID    string
		Title       string
		Description string
		Manager     Contact
		Tags        []string
		IncludedIDs []string
	}

	tbl, err := Use[Alert](db, "alerts")
	if err != nil {
		t.Fatal(err)
	}

	alert := Alert{
		TenantID:    "tenant-123",
		Title:       "Power BI Pro and licenses expire",
		Description: makeString(8000),
		Manager: Contact{
			Name:  "Peter Black",
			Email: "peter@example.com",
			Notes: makeString(3000),
		},
		Tags:        []string{"tag1", "tag2", "tag3"},
		IncludedIDs: []string{makeString(36), makeString(36), makeString(36)},
	}

	id, err := tbl.Insert(&alert)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if rec.TenantID != "tenant-123" {
		t.Errorf("tenantid mismatch")
	}
	if len(rec.Description) != 8000 {
		t.Errorf("description length: got %d, expected 8000", len(rec.Description))
	}
	if rec.Manager.Name != "Peter Black" {
		t.Errorf("manager name mismatch")
	}
	if len(rec.Manager.Notes) != 3000 {
		t.Errorf("manager notes length: got %d, expected 3000", len(rec.Manager.Notes))
	}
	if len(rec.Tags) != 3 {
		t.Errorf("tags length: got %d, expected 3", len(rec.Tags))
	}
	if len(rec.IncludedIDs) != 3 {
		t.Errorf("includedids length: got %d, expected 3", len(rec.IncludedIDs))
	}

	db.Close()
	os.Remove("/tmp/test_nested_large.db")
}

func makeString(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}

type TimeRecord struct {
	ID        uint32 `db:"id,primary"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestTimeFieldRoundTrip(t *testing.T) {
	os.Remove("/tmp/test_time_basic.db")
	defer os.Remove("/tmp/test_time_basic.db")

	db, err := Open("/tmp/test_time_basic.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TimeRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		ts   time.Time
	}{
		{"nanosecond_precision", time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)},
		{"microsecond_precision", time.Date(2024, 1, 1, 12, 0, 0, 500000000, time.UTC)},
		{"millisecond_precision", time.Date(2023, 12, 31, 23, 59, 59, 999000000, time.UTC)},
		{"zero_nano", time.Date(2024, 3, 15, 8, 30, 0, 0, time.UTC)},
		{"epoch", time.Unix(0, 0).UTC()},
		{"pre_epoch", time.Date(1969, 12, 31, 23, 59, 59, 123456789, time.UTC)},
		{"distant_future", time.Date(2100, 1, 1, 0, 0, 0, 999999999, time.UTC)},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &TimeRecord{CreatedAt: tc.ts, UpdatedAt: tc.ts}
			id, err := tbl.Insert(rec)
			if err != nil {
				t.Fatalf("Insert %s failed: %v", tc.name, err)
			}

			got, err := tbl.Get(id)
			if err != nil {
				t.Fatalf("Get %s failed: %v", tc.name, err)
			}

			if !got.CreatedAt.Equal(tc.ts) {
				t.Errorf("case %d %s: CreatedAt mismatch: got %v (nano=%d), want %v (nano=%d)",
					i, tc.name, got.CreatedAt, got.CreatedAt.Nanosecond(), tc.ts, tc.ts.Nanosecond())
			}
			if !got.UpdatedAt.Equal(tc.ts) {
				t.Errorf("case %d %s: UpdatedAt mismatch: got %v (nano=%d), want %v (nano=%d)",
					i, tc.name, got.UpdatedAt, got.UpdatedAt.Nanosecond(), tc.ts, tc.ts.Nanosecond())
			}
		})
	}
}

func TestTimeFieldUpdate(t *testing.T) {
	os.Remove("/tmp/test_time_update.db")
	defer os.Remove("/tmp/test_time_update.db")

	db, err := Open("/tmp/test_time_update.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TimeRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 1, 10, 0, 0, 123456789, time.UTC)
	ts2 := time.Date(2024, 6, 15, 14, 30, 0, 987654321, time.UTC)

	id, err := tbl.Insert(&TimeRecord{CreatedAt: ts1, UpdatedAt: ts1})
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Update(id, &TimeRecord{ID: id, CreatedAt: ts2, UpdatedAt: ts2})
	if err != nil {
		t.Fatal(err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	if !got.CreatedAt.Equal(ts2) {
		t.Errorf("CreatedAt after update: got %v, want %v", got.CreatedAt, ts2)
	}
	if !got.UpdatedAt.Equal(ts2) {
		t.Errorf("UpdatedAt after update: got %v, want %v", got.UpdatedAt, ts2)
	}
}

func TestTimeFieldPersistence(t *testing.T) {
	os.Remove("/tmp/test_time_persist.db")

	ts := time.Date(2024, 7, 20, 15, 45, 30, 500999999, time.UTC)

	{
		db, err := Open("/tmp/test_time_persist.db")
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := Use[TimeRecord](db, "test")
		if err != nil {
			t.Fatal(err)
		}
		_, err = tbl.Insert(&TimeRecord{CreatedAt: ts, UpdatedAt: ts})
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	{
		db2, err := Open("/tmp/test_time_persist.db")
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()
		tbl2, err := Use[TimeRecord](db2, "test")
		if err != nil {
			t.Fatal(err)
		}
		got, err := tbl2.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if !got.CreatedAt.Equal(ts) {
			t.Errorf("CreatedAt after reopen: got %v (nano=%d), want %v (nano=%d)",
				got.CreatedAt, got.CreatedAt.Nanosecond(), ts, ts.Nanosecond())
		}
		if !got.UpdatedAt.Equal(ts) {
			t.Errorf("UpdatedAt after reopen: got %v (nano=%d), want %v (nano=%d)",
				got.UpdatedAt, got.UpdatedAt.Nanosecond(), ts, ts.Nanosecond())
		}
	}

	os.Remove("/tmp/test_time_persist.db")
}
