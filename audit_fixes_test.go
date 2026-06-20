package embeddb

import (
	"os"
	"testing"
	"time"
)

func TestWALReplayThenWrite(t *testing.T) {
	os.Remove("/tmp/test_wal_replay_write.db")
	defer os.Remove("/tmp/test_wal_replay_write.db")
	defer os.Remove("/tmp/test_wal_replay_write.db.wal")

	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("/tmp/test_wal_replay_write.db", OpenOptions{WAL: true})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tbl.Insert(&User{Name: "Alice"}); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open("/tmp/test_wal_replay_write.db", OpenOptions{WAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tbl.Get(1)
	if err != nil {
		t.Fatalf("Get after WAL replay: %v", err)
	}
	if rec.Name != "Alice" {
		t.Errorf("expected Alice, got %s", rec.Name)
	}

	if _, err := tbl.Insert(&User{Name: "Bob"}); err != nil {
		t.Fatalf("Insert after WAL replay should not panic: %v", err)
	}

	rec, err = tbl.Get(2)
	if err != nil {
		t.Fatalf("Get new record after WAL replay: %v", err)
	}
	if rec.Name != "Bob" {
		t.Errorf("expected Bob, got %s", rec.Name)
	}
}

func TestExplicitIndexDeleteUpdate(t *testing.T) {
	os.Remove("/tmp/test_explicit_idx.db")
	defer os.Remove("/tmp/test_explicit_idx.db")

	type User struct {
		ID    uint32 `db:"id,primary"`
		Name  string
		Email string
	}

	db, err := Open("/tmp/test_explicit_idx.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	if err := tbl.CreateIndex("Email"); err != nil {
		t.Fatal(err)
	}

	if _, err := tbl.Insert(&User{Name: "Alice", Email: "alice@test.com"}); err != nil {
		t.Fatal(err)
	}

	results, err := tbl.Query("Email", "alice@test.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Name != "Alice" {
		t.Fatalf("expected Alice, got %v", results)
	}

	if err := tbl.Update(1, &User{Name: "Alice Updated", Email: "alice2@test.com"}); err != nil {
		t.Fatal(err)
	}

	results, err = tbl.Query("Email", "alice@test.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("old email should have 0 results after update, got %d", len(results))
	}

	results, err = tbl.Query("Email", "alice2@test.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Name != "Alice Updated" {
		t.Errorf("expected Alice Updated at new email, got %v", results)
	}

	if err := tbl.Delete(1); err != nil {
		t.Fatal(err)
	}

	results, err = tbl.Query("Email", "alice2@test.com")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("email should have 0 results after delete, got %d", len(results))
	}
}

func TestRollbackReopenStaleRoot(t *testing.T) {
	os.Remove("/tmp/test_rollback_reopen.db")
	defer os.Remove("/tmp/test_rollback_reopen.db")

	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("/tmp/test_rollback_reopen.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tbl.Insert(&User{Name: "Alice"}); err != nil {
		t.Fatal(err)
	}

	tx := db.Begin()
	for i := 0; i < 20; i++ {
		tbl.Insert(&User{Name: "temp"})
	}
	tx.Rollback()

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open("/tmp/test_rollback_reopen.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tbl.Get(1)
	if err != nil {
		t.Fatalf("Get after rollback+reopen: %v", err)
	}
	if rec.Name != "Alice" {
		t.Errorf("expected Alice, got %s", rec.Name)
	}

	count := tbl.Count()
	if count != 1 {
		t.Errorf("expected count 1 after rollback, got %d", count)
	}
}

func TestLikeWildcardUnderscore(t *testing.T) {
	os.Remove("/tmp/test_like_underscore.db")
	defer os.Remove("/tmp/test_like_underscore.db")

	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("/tmp/test_like_underscore.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&User{Name: "cat"})
	tbl.Insert(&User{Name: "cot"})
	tbl.Insert(&User{Name: "cut"})
	tbl.Insert(&User{Name: "cart"})
	tbl.Insert(&User{Name: "cute"})

	tests := []struct {
		pattern string
		want    int
	}{
		{"c_t", 3},
		{"c__t", 1},
		{"c%", 5},
		{"%t", 4},
		{"%a%", 2},
		{"c_t%", 4},
	}

	for _, tt := range tests {
		results, err := tbl.QueryLike("Name", tt.pattern)
		if err != nil {
			t.Fatalf("QueryLike(%q): %v", tt.pattern, err)
		}
		if len(results) != tt.want {
			t.Errorf("QueryLike(%q): got %d results, want %d", tt.pattern, len(results), tt.want)
		}
	}
}

func TestSchemaChangeDetectionInUse(t *testing.T) {
	os.Remove("/tmp/test_schema_change.db")
	defer os.Remove("/tmp/test_schema_change.db")

	type V1 struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	type V2 struct {
		ID    uint32 `db:"id,primary"`
		Name  string
		Email string
	}

	db, err := Open("/tmp/test_schema_change.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}

	tbl1, err := Use[V1](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	tbl1.Insert(&V1{Name: "Alice"})

	db.Close()

	db, err = Open("/tmp/test_schema_change.db", OpenOptions{AutoIndex: false, Migrate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl2, err := Use[V2](db, "users")
	if err != nil {
		t.Fatalf("Use[V2] after schema change should work with migrate: %v", err)
	}

	rec, err := tbl2.Get(1)
	if err != nil {
		t.Fatalf("Get after schema change: %v", err)
	}
	if rec.Name != "Alice" {
		t.Errorf("expected Alice, got %s", rec.Name)
	}
}

func TestInsertManyBulkMemoryMode(t *testing.T) {
	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("", OpenOptions{StorageMode: StorageMemory})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	recs := make([]*User, 100)
	for i := range recs {
		recs[i] = &User{Name: "bulk-" + string(rune('A'+i%26)) + string(rune('A'+i/26))}
	}

	ids, err := tbl.InsertManyBulk(recs)
	if err != nil {
		t.Fatalf("InsertManyBulk in memory mode: %v", err)
	}
	if len(ids) != 100 {
		t.Errorf("expected 100 ids, got %d", len(ids))
	}

	for i, id := range ids {
		rec, err := tbl.Get(id)
		if err != nil {
			t.Fatalf("Get(%d) after bulk insert: %v", id, err)
		}
		if rec.Name != recs[i].Name {
			t.Errorf("record %d: expected %s, got %s", id, recs[i].Name, rec.Name)
		}
	}
}

func TestDeactivateRecordErrorPropagation(t *testing.T) {
	os.Remove("/tmp/test_deactivate_err.db")
	defer os.Remove("/tmp/test_deactivate_err.db")

	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("/tmp/test_deactivate_err.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&User{Name: "Alice"})

	db.Close()

	db2, err := Open("/tmp/test_deactivate_err.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	tbl2, err := Use[User](db2, "users")
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tbl2.Get(1)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if rec.Name != "Alice" {
		t.Errorf("expected Alice, got %s", rec.Name)
	}

	if err := tbl2.Delete(1); err != nil {
		t.Fatalf("Delete should succeed: %v", err)
	}

	_, err = tbl2.Get(1)
	if err == nil {
		t.Error("Get after delete should fail")
	}
}

func TestDeleteFromNodeErrorPropagation(t *testing.T) {
	os.Remove("/tmp/test_delete_err.db")
	defer os.Remove("/tmp/test_delete_err.db")

	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("/tmp/test_delete_err.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		tbl.Insert(&User{Name: "user"})
	}

	for i := uint32(1); i <= 50; i++ {
		if err := tbl.Delete(i); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}

	count := tbl.Count()
	if count != 50 {
		t.Errorf("expected count 50 after deletions, got %d", count)
	}

	for i := uint32(51); i <= 100; i++ {
		rec, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("Get(%d) after deletions: %v", i, err)
		}
		if rec.Name != "user" {
			t.Errorf("record %d: expected user, got %s", i, rec.Name)
		}
	}
}

func TestTxCommitThenReopen(t *testing.T) {
	os.Remove("/tmp/test_tx_reopen.db")
	defer os.Remove("/tmp/test_tx_reopen.db")

	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("/tmp/test_tx_reopen.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	tx := db.Begin()
	tbl.Insert(&User{Name: "Alice"})
	tbl.Insert(&User{Name: "Bob"})
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	db.Close()

	db, err = Open("/tmp/test_tx_reopen.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	if tbl.Count() != 2 {
		t.Errorf("expected count 2 after commit+reopen, got %d", tbl.Count())
	}

	rec, err := tbl.Get(1)
	if err != nil || rec.Name != "Alice" {
		t.Errorf("expected Alice, got %v err=%v", rec, err)
	}

	rec, err = tbl.Get(2)
	if err != nil || rec.Name != "Bob" {
		t.Errorf("expected Bob, got %v err=%v", rec, err)
	}
}

func TestTxRollbackThenReopen(t *testing.T) {
	os.Remove("/tmp/test_tx_rollback_reopen.db")
	defer os.Remove("/tmp/test_tx_rollback_reopen.db")

	type User struct {
		ID   uint32 `db:"id,primary"`
		Name string
	}

	db, err := Open("/tmp/test_tx_rollback_reopen.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&User{Name: "Alice"})

	tx := db.Begin()
	tbl.Insert(&User{Name: "Bob"})
	tbl.Insert(&User{Name: "Charlie"})
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	db.Close()
	time.Sleep(100 * time.Millisecond)

	db, err = Open("/tmp/test_tx_rollback_reopen.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[User](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	if tbl.Count() != 1 {
		t.Errorf("expected count 1 after rollback+reopen, got %d", tbl.Count())
	}

	rec, err := tbl.Get(1)
	if err != nil || rec.Name != "Alice" {
		t.Errorf("expected Alice, got %v err=%v", rec, err)
	}
}