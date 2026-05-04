package embeddb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type BenchUser struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Email string
	Age   int
}

func TestBulkInsertCorrectness(t *testing.T) {
	os.Remove("/tmp/test_bulk_correct.db")
	defer os.Remove("/tmp/test_bulk_correct.db")

	db, err := Open("/tmp/test_bulk_correct.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[BenchUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	records := make([]*BenchUser, 100)
	for i := 0; i < 100; i++ {
		records[i] = &BenchUser{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@test.com", i),
			Age:   20 + i%50,
		}
	}

	ids, err := tbl.InsertManyBulk(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 100 {
		t.Fatalf("expected 100 ids, got %d", len(ids))
	}

	for i, id := range ids {
		if id != uint32(i+1) {
			t.Fatalf("expected id %d, got %d", i+1, id)
		}
	}

	for i := uint32(1); i <= 100; i++ {
		user, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get user %d: %v", i, err)
		}
		if user.Name != fmt.Sprintf("User%d", i-1) {
			t.Errorf("user %d: expected name 'User%d', got '%s'", i, i-1, user.Name)
		}
	}

	results, err := tbl.Query("Name", "User50")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for User50, got %d", len(results))
	}
}

func TestBulkInsertRestartNoWAL(t *testing.T) {
	os.Remove("/tmp/test_bulk_restart_nowal.db")
	defer os.Remove("/tmp/test_bulk_restart_nowal.db")

	db, err := Open("/tmp/test_bulk_restart_nowal.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[BenchUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	records := make([]*BenchUser, 500)
	for i := 0; i < 500; i++ {
		records[i] = &BenchUser{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@test.com", i),
			Age:   20 + i%50,
		}
	}

	_, err = tbl.InsertManyBulk(records)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Before close: index root=%d, count=%d", db.database.index.RootOffset(), tbl.Count())

	user, err := tbl.Get(1)
	if err != nil {
		t.Fatalf("failed to get user 1 before close: %v", err)
	}
	t.Logf("User 1 before close: %+v", user)

	err = db.database.flush()
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	t.Logf("After flush: index root=%d, count=%d", db.database.index.RootOffset(), tbl.Count())

	db.Close()

	db, err = Open("/tmp/test_bulk_restart_nowal.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[BenchUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("After open: index root=%d, count=%d", db.database.index.RootOffset(), tbl.Count())
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(1); i <= 500; i++ {
		user, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get user %d after restart: %v", i, err)
		}
		if user.Name != fmt.Sprintf("User%d", i-1) {
			t.Errorf("user %d: expected name 'User%d', got '%s'", i, i-1, user.Name)
		}
	}

	count := tbl.Count()
	if count != 500 {
		t.Errorf("expected 500 records, got %d", count)
	}
}

func TestBulkInsertRestart(t *testing.T) {
	os.Remove("/tmp/test_bulk_restart.db")
	os.Remove("/tmp/test_bulk_restart.db.wal")
	defer os.Remove("/tmp/test_bulk_restart.db")
	defer os.Remove("/tmp/test_bulk_restart.db.wal")

	db, err := Open("/tmp/test_bulk_restart.db", OpenOptions{WAL: true})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[BenchUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	records := make([]*BenchUser, 500)
	for i := 0; i < 500; i++ {
		records[i] = &BenchUser{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@test.com", i),
			Age:   20 + i%50,
		}
	}

	_, err = tbl.InsertManyBulk(records)
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db, err = Open("/tmp/test_bulk_restart.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[BenchUser](db, "users")
	if err != nil {
		t.Fatal(err)
	}

	count := tbl.Count()
	t.Logf("After restart, count=%d (expected 500)", count)

	user, err := tbl.Get(1)
	if err != nil {
		t.Fatalf("failed to get user 1 after restart: %v", err)
	}
	t.Logf("User 1: %+v", user)

	for i := uint32(2); i <= 500; i++ {
		u, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get user %d after restart: %v", i, err)
		}
		if u.Name != fmt.Sprintf("User%d", i-1) {
			t.Errorf("user %d: expected name 'User%d', got '%s'", i, i-1, u.Name)
		}
	}

	count = tbl.Count()
	if count != 500 {
		t.Errorf("expected 500 records, got %d", count)
	}
}

func BenchmarkInsertMany(b *testing.B) {
	dbPath := "/tmp/bench_insertmany.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	b.N = 1

	for _, n := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("InsertMany_%d", n), func(b *testing.B) {
			os.Remove(dbPath)
			db, _ := Open(dbPath)
			tbl, _ := Use[BenchUser](db, "users")

			records := make([]*BenchUser, n)
			for j := 0; j < n; j++ {
				records[j] = &BenchUser{
					Name:  fmt.Sprintf("User%d", j),
					Email: fmt.Sprintf("user%d@test.com", j),
					Age:   j % 100,
				}
			}

			start := time.Now()
			_, err := tbl.InsertMany(records)
			elapsed := time.Since(start)

			if err != nil {
				b.Fatal(err)
			}
			b.Logf("InsertMany %d: %v (%.0f/sec)", n, elapsed, float64(n)/elapsed.Seconds())
			db.Close()
		})

		b.Run(fmt.Sprintf("InsertManyBulk_%d", n), func(b *testing.B) {
			os.Remove(dbPath)
			db, _ := Open(dbPath)
			tbl, _ := Use[BenchUser](db, "users")

			records := make([]*BenchUser, n)
			for j := 0; j < n; j++ {
				records[j] = &BenchUser{
					Name:  fmt.Sprintf("User%d", j),
					Email: fmt.Sprintf("user%d@test.com", j),
					Age:   j % 100,
				}
			}

			start := time.Now()
			_, err := tbl.InsertManyBulk(records)
			elapsed := time.Since(start)

			if err != nil {
				b.Fatal(err)
			}
			b.Logf("InsertManyBulk %d: %v (%.0f/sec)", n, elapsed, float64(n)/elapsed.Seconds())
			db.Close()
		})
	}
}
