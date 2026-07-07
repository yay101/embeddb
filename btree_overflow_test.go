package embeddb

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

type BTreeLargeKeyRec struct {
	ID    uint32 `db:"id,primary"`
	Name  string
	Data  string
	Notes string
}

func TestBTreeLargeSecondaryKeys(t *testing.T) {
	path := "/tmp/test_btree_large_seckeys.db"
	os.Remove(path)
	defer os.Remove(path)

	db, err := Open(path, OpenOptions{AutoIndex: Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[BTreeLargeKeyRec](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Data is a non-primary, non-slice field, so auto-indexing builds a secondary
	// key whose value is the (252-byte-truncated) field value. With a 300-byte
	// value the secondary key is 1+1+1+4("Data")+252+4 = 263 bytes, producing a
	// 273-byte cell. A leaf page holds ~14 such cells; inserting many records
	// forces repeated splits with variable-length keys.
	longData := strings.Repeat("x", 300)
	const n = 500
	for i := 0; i < n; i++ {
		if _, err := tbl.Insert(&BTreeLargeKeyRec{
			Name:  fmt.Sprintf("n%d", i),
			Notes: fmt.Sprintf("o%d", i),
			Data:  longData,
		}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if got := tbl.Count(); got != n {
		t.Fatalf("expected %d records, got %d", n, got)
	}

	for i := uint32(1); i <= n; i++ {
		rec, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if rec.Data != longData {
			t.Errorf("rec %d: data mismatch", i)
		}
	}

	res, err := tbl.Query("Name", "n250")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 1 {
		t.Errorf("expected 1 result for Name=n250, got %d", len(res))
	}
}

func TestBTreeLargeSecondaryKeysCompressed(t *testing.T) {
	path := "/tmp/test_btree_large_seckeys_compress.db"
	os.Remove(path)
	defer os.Remove(path)

	db, err := Open(path, OpenOptions{AutoIndex: Bool(true), Compression: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[BTreeLargeKeyRec](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	largeData := strings.Repeat("xyz123", 200)
	const n = 300
	for i := 0; i < n; i++ {
		if _, err := tbl.Insert(&BTreeLargeKeyRec{
			Name:  fmt.Sprintf("item%d", i),
			Notes: fmt.Sprintf("note%d", i),
			Data:  largeData,
		}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	for i := uint32(1); i <= n; i++ {
		rec, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if rec.Data != largeData {
			t.Errorf("rec %d: data mismatch", i)
		}
	}

	res, err := tbl.Query("Name", "item150")
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Errorf("expected 1 result, got %d", len(res))
	}
}

func TestBTreeLargeSecondaryKeysRestart(t *testing.T) {
	path := "/tmp/test_btree_large_seckeys_restart.db"
	os.Remove(path)
	defer os.Remove(path)

	{
		db, err := Open(path, OpenOptions{AutoIndex: Bool(true)})
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := Use[BTreeLargeKeyRec](db, "test")
		if err != nil {
			t.Fatal(err)
		}
		longData := strings.Repeat("x", 300)
		for i := 0; i < 200; i++ {
			if _, err := tbl.Insert(&BTreeLargeKeyRec{
				Name:  fmt.Sprintf("n%d", i),
				Notes: fmt.Sprintf("o%d", i),
				Data:  longData,
			}); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	{
		db, err := Open(path, OpenOptions{AutoIndex: Bool(true)})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		tbl, err := Use[BTreeLargeKeyRec](db, "test")
		if err != nil {
			t.Fatal(err)
		}
		if got := tbl.Count(); got != 200 {
			t.Fatalf("expected 200 records after restart, got %d", got)
		}
		longData := strings.Repeat("x", 300)
		for i := uint32(1); i <= 200; i++ {
			rec, err := tbl.Get(i)
			if err != nil {
				t.Fatalf("get %d: %v", i, err)
			}
			if rec.Data != longData {
				t.Errorf("rec %d: data mismatch", i)
			}
		}
		// Insert more after restart to keep splitting
		for i := 200; i < 400; i++ {
			if _, err := tbl.Insert(&BTreeLargeKeyRec{
				Name:  fmt.Sprintf("n%d", i),
				Notes: fmt.Sprintf("o%d", i),
				Data:  longData,
			}); err != nil {
				t.Fatalf("insert %d after restart: %v", i, err)
			}
		}
		if got := tbl.Count(); got != 400 {
			t.Fatalf("expected 400 records, got %d", got)
		}
	}
}