package embeddb

import (
	"os"
	"testing"
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
