package embeddb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type CloseLogEntry struct {
	ID        uint32    `db:"id,primary"`
	Level     string    `db:"index"`
	Message   string    `db:"index"`
	Source    string
	Timestamp time.Time
}

func makeCloseLogRecords(n int) []*CloseLogEntry {
	recs := make([]*CloseLogEntry, n)
	for i := 0; i < n; i++ {
		recs[i] = &CloseLogEntry{
			Level:     []string{"INFO", "WARN", "ERROR"}[i%3],
			Message:   fmt.Sprintf("log message %d", i),
			Source:    "svc",
			Timestamp: time.Now(),
		}
	}
	return recs
}

func TestFastCloseNoWALRoundtrip(t *testing.T) {
	path := "/tmp/test_fast_close_nowal.db"
	os.Remove(path)
	defer os.Remove(path)

	db, err := Open(path, OpenOptions{AutoIndex: Bool(true), CompactOnClose: false})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}

	const n = 2000
	recs := makeCloseLogRecords(n)
	ids, err := tbl.InsertManyBulk(recs)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != n {
		t.Fatalf("expected %d ids, got %d", n, len(ids))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(path, OpenOptions{AutoIndex: Bool(true), CompactOnClose: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}

	if got := tbl.Count(); got != n {
		t.Fatalf("expected %d records after reopen, got %d", n, got)
	}

	for i := uint32(1); i <= n; i++ {
		rec, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if rec.Message != fmt.Sprintf("log message %d", int(i)-1) {
			t.Errorf("rec %d: expected 'log message %d', got %q", i, int(i)-1, rec.Message)
		}
	}

	res, err := tbl.Query("Level", "ERROR")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != n/3 {
		t.Errorf("expected %d ERROR entries, got %d", n/3, len(res))
	}
}

func TestFastCloseWALRoundtrip(t *testing.T) {
	path := "/tmp/test_fast_close_wal.db"
	os.Remove(path)
	os.Remove(path + ".wal")
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	db, err := Open(path, OpenOptions{WAL: true, CompactOnClose: false})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}

	const n = 2000
	recs := makeCloseLogRecords(n)
	if _, err := tbl.InsertManyBulk(recs); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(path, OpenOptions{WAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}

	if got := tbl.Count(); got != n {
		t.Fatalf("expected %d records after reopen, got %d", n, got)
	}

	for i := uint32(1); i <= n; i++ {
		rec, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if rec.Level != []string{"INFO", "WARN", "ERROR"}[(int(i)-1)%3] {
			t.Errorf("rec %d: level mismatch", i)
		}
	}
}

func TestFastCloseFollowedByCompact(t *testing.T) {
	path := "/tmp/test_fast_close_then_compact.db"
	os.Remove(path)
	defer os.Remove(path)

	db, err := Open(path, OpenOptions{AutoIndex: Bool(true), CompactOnClose: false})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}

	const n = 1000
	if _, err := tbl.InsertManyBulk(makeCloseLogRecords(n)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(path, OpenOptions{AutoIndex: Bool(true), CompactOnClose: true})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err = Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl.Count(); got != n {
		t.Fatalf("expected %d records after reopen, got %d", n, got)
	}

	if err := db.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after sync: %v", err)
	}

	db2, err := Open(path, OpenOptions{AutoIndex: Bool(true), CompactOnClose: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	tbl2, err := Use[CloseLogEntry](db2, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl2.Count(); got != n {
		t.Fatalf("expected %d records after compaction+reopen, got %d", n, got)
	}
	for i := uint32(1); i <= n; i++ {
		if _, err := tbl2.Get(i); err != nil {
			t.Fatalf("get %d after compaction: %v", i, err)
		}
	}
}

func TestCompactOnCloseTrueStillCompacts(t *testing.T) {
	path := "/tmp/test_compact_on_close_true.db"
	os.Remove(path)
	defer os.Remove(path)

	db, err := Open(path, OpenOptions{AutoIndex: Bool(true), CompactOnClose: true})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}

	const n = 1000
	if _, err := tbl.InsertManyBulk(makeCloseLogRecords(n)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tbl, err = Use[CloseLogEntry](db, "logs")
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl.Count(); got != n {
		t.Fatalf("expected %d records, got %d", n, got)
	}
	for i := uint32(1); i <= n; i++ {
		if _, err := tbl.Get(i); err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
	}
}

func TestFastCloseTimingDifference(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing test in -short mode")
	}
	const n = 20000

	pathFast := "/tmp/test_fast_close_timing.db"
	pathCompact := "/tmp/test_compact_close_timing.db"
	os.Remove(pathFast)
	os.Remove(pathCompact)
	defer os.Remove(pathFast)
	defer os.Remove(pathCompact)

	runBatch := func(path string, compact bool) time.Duration {
		db, err := Open(path, OpenOptions{AutoIndex: Bool(true), CompactOnClose: compact})
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := Use[CloseLogEntry](db, "logs")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tbl.InsertManyBulk(makeCloseLogRecords(n)); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}

	fastDur := runBatch(pathFast, false)
	compactDur := runBatch(pathCompact, true)

	t.Logf("close with %d records: fast=%v compact=%v (%.1fx faster)",
		n, fastDur, compactDur, float64(compactDur)/float64(fastDur))

	if fastDur > compactDur {
		t.Errorf("expected fast close (%v) to be quicker than compact (%v)", fastDur, compactDur)
	}
}