package embeddb

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type cmpRecord struct {
	ID    uint32 `db:"id,primary"`
	Name  string
	Age   int32
	Email string
}

type cmpRecordIndexed struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"name,index"`
	Age   int32
	Email string
}

func newCmpRecord(i int) *cmpRecord {
	return &cmpRecord{
		Name:  fmt.Sprintf("user-%06d", i),
		Age:   int32(20 + i%50),
		Email: fmt.Sprintf("user-%06d@example.com", i),
	}
}

func newCmpRecordIndexed(i int) *cmpRecordIndexed {
	return &cmpRecordIndexed{
		Name:  fmt.Sprintf("user-%06d", i),
		Age:   int32(20 + i%50),
		Email: fmt.Sprintf("user-%06d@example.com", i),
	}
}

func setupSQLiteMem(tb testing.TB) (*sql.DB, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT,
		age INTEGER,
		email TEXT
	)`)
	if err != nil {
		tb.Fatal(err)
	}
	return db, func() { db.Close() }
}

func setupSQLiteFile(tb testing.TB) (*sql.DB, func()) {
	path := "/tmp/bench_sqlite_cmp.db"
	os.Remove(path)
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT,
		age INTEGER,
		email TEXT
	)`)
	if err != nil {
		tb.Fatal(err)
	}
	return db, func() { db.Close(); os.Remove(path) }
}

func setupSQLiteMemIndexed(tb testing.TB) (*sql.DB, func()) {
	db, cleanup := setupSQLiteMem(tb)
	_, err := db.Exec(`CREATE INDEX idx_name ON records(name)`)
	if err != nil {
		tb.Fatal(err)
	}
	return db, cleanup
}

func setupSQLiteFileIndexed(tb testing.TB) (*sql.DB, func()) {
	db, cleanup := setupSQLiteFile(tb)
	_, err := db.Exec(`CREATE INDEX idx_name ON records(name)`)
	if err != nil {
		tb.Fatal(err)
	}
	return db, cleanup
}

func setupEmbedDBMem(tb testing.TB) (*DB, func()) {
	path := "/tmp/bench_embed_mem.db"
	os.Remove(path)
	db, err := Open(path, OpenOptions{StorageMode: StorageMemory, AutoIndex: true})
	if err != nil {
		tb.Fatal(err)
	}
	return db, func() { db.Close(); os.Remove(path) }
}

func setupEmbedDBFile(tb testing.TB) (*DB, func()) {
	path := "/tmp/bench_embed_file.db"
	os.Remove(path)
	db, err := Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	return db, func() { db.Close(); os.Remove(path) }
}

func sqliteInsertPrepared(stmt *sql.Stmt, i int) error {
	_, err := stmt.Exec(
		fmt.Sprintf("user-%06d", i),
		20+i%50,
		fmt.Sprintf("user-%06d@example.com", i),
	)
	return err
}

func sqliteInsert(db *sql.DB, i int) error {
	_, err := db.Exec(
		`INSERT INTO records (name, age, email) VALUES (?, ?, ?)`,
		fmt.Sprintf("user-%06d", i),
		20+i%50,
		fmt.Sprintf("user-%06d@example.com", i),
	)
	return err
}

// --- Insert ---

func BenchmarkCmpSQLiteMemInsert(b *testing.B) {
	db, cleanup := setupSQLiteMem(b)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sqliteInsert(db, i); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpSQLiteFileInsert(b *testing.B) {
	db, cleanup := setupSQLiteFile(b)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sqliteInsert(db, i); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBMemInsert(b *testing.B) {
	db, cleanup := setupEmbedDBMem(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tbl.Insert(newCmpRecord(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBFileInsert(b *testing.B) {
	db, cleanup := setupEmbedDBFile(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tbl.Insert(newCmpRecord(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Batch Insert ---

func sqliteBatchInsert(db *sql.DB, n int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO records (name, age, email) VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i := 0; i < n; i++ {
		if err := sqliteInsertPrepared(stmt, i); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func BenchmarkCmpSQLiteMemBatch1000(b *testing.B) {
	db, cleanup := setupSQLiteMem(b)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sqliteBatchInsert(db, 1000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpSQLiteFileBatch1000(b *testing.B) {
	db, cleanup := setupSQLiteFile(b)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sqliteBatchInsert(db, 1000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBMemBatch1000(b *testing.B) {
	db, cleanup := setupEmbedDBMem(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records := make([]*cmpRecord, 1000)
		for j := 0; j < 1000; j++ {
			records[j] = newCmpRecord(i*1000 + j)
		}
		if _, err := tbl.InsertMany(records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBFileBatch1000(b *testing.B) {
	db, cleanup := setupEmbedDBFile(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records := make([]*cmpRecord, 1000)
		for j := 0; j < 1000; j++ {
			records[j] = newCmpRecord(i*1000 + j)
		}
		if _, err := tbl.InsertMany(records); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Bulk Insert ---

func BenchmarkCmpEmbedDBMemBulk1000(b *testing.B) {
	db, cleanup := setupEmbedDBMem(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records := make([]*cmpRecord, 1000)
		for j := 0; j < 1000; j++ {
			records[j] = newCmpRecord(i*1000 + j)
		}
		if _, err := tbl.InsertManyBulk(records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBFileBulk1000(b *testing.B) {
	db, cleanup := setupEmbedDBFile(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records := make([]*cmpRecord, 1000)
		for j := 0; j < 1000; j++ {
			records[j] = newCmpRecord(i*1000 + j)
		}
		if _, err := tbl.InsertManyBulk(records); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Get (point lookup by primary key) ---

func preloadData(tb testing.TB, n int, insertFn func(int) error) {
	for i := 1; i <= n; i++ {
		if err := insertFn(i); err != nil {
			tb.Fatal(err)
		}
	}
}

func BenchmarkCmpSQLiteMemGet(b *testing.B) {
	db, cleanup := setupSQLiteMem(b)
	defer cleanup()
	preloadData(b, 10000, func(i int) error { return sqliteInsert(db, i) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := (i % 10000) + 1
		var name string
		if err := db.QueryRow(`SELECT name FROM records WHERE id = ?`, id).Scan(&name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpSQLiteFileGet(b *testing.B) {
	db, cleanup := setupSQLiteFile(b)
	defer cleanup()
	preloadData(b, 10000, func(i int) error { return sqliteInsert(db, i) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := (i % 10000) + 1
		var name string
		if err := db.QueryRow(`SELECT name FROM records WHERE id = ?`, id).Scan(&name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBMemGet(b *testing.B) {
	db, cleanup := setupEmbedDBMem(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 10000, func(i int) error {
		_, err := tbl.Insert(newCmpRecord(i))
		return err
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint32((i % 10000) + 1)
		if _, err := tbl.Get(id); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBFileGet(b *testing.B) {
	db, cleanup := setupEmbedDBFile(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 10000, func(i int) error {
		_, err := tbl.Insert(newCmpRecord(i))
		return err
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint32((i % 10000) + 1)
		if _, err := tbl.Get(id); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Query (indexed field lookup) ---

func BenchmarkCmpSQLiteMemQueryIndexed(b *testing.B) {
	db, cleanup := setupSQLiteMemIndexed(b)
	defer cleanup()
	preloadData(b, 10000, func(i int) error { return sqliteInsert(db, i) })
	b.ResetTimer()
	names := []string{"user-000042", "user-001000", "user-005000", "user-009999"}
	for i := 0; i < b.N; i++ {
		name := names[i%len(names)]
		var age int
		if err := db.QueryRow(`SELECT age FROM records WHERE name = ?`, name).Scan(&age); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpSQLiteFileQueryIndexed(b *testing.B) {
	db, cleanup := setupSQLiteFileIndexed(b)
	defer cleanup()
	preloadData(b, 10000, func(i int) error { return sqliteInsert(db, i) })
	b.ResetTimer()
	names := []string{"user-000042", "user-001000", "user-005000", "user-009999"}
	for i := 0; i < b.N; i++ {
		name := names[i%len(names)]
		var age int
		if err := db.QueryRow(`SELECT age FROM records WHERE name = ?`, name).Scan(&age); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBMemQueryIndexed(b *testing.B) {
	db, cleanup := setupEmbedDBMem(b)
	defer cleanup()
	tbl, err := Use[cmpRecordIndexed](db, "records_i")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 10000, func(i int) error {
		_, err := tbl.Insert(newCmpRecordIndexed(i))
		return err
	})
	b.ResetTimer()
	names := []string{"user-000042", "user-001000", "user-005000", "user-009999"}
	for i := 0; i < b.N; i++ {
		name := names[i%len(names)]
		if _, err := tbl.Query("Name", name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCmpEmbedDBFileQueryIndexed(b *testing.B) {
	db, cleanup := setupEmbedDBFile(b)
	defer cleanup()
	tbl, err := Use[cmpRecordIndexed](db, "records_i")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 10000, func(i int) error {
		_, err := tbl.Insert(newCmpRecordIndexed(i))
		return err
	})
	b.ResetTimer()
	names := []string{"user-000042", "user-001000", "user-005000", "user-009999"}
	for i := 0; i < b.N; i++ {
		name := names[i%len(names)]
		if _, err := tbl.Query("Name", name); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Scan (full table read) ---

func BenchmarkCmpSQLiteMemScan(b *testing.B) {
	db, cleanup := setupSQLiteMem(b)
	defer cleanup()
	preloadData(b, 10000, func(i int) error { return sqliteInsert(db, i) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT id, name, age, email FROM records`)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var id int
			var name, email string
			var age int
			if err := rows.Scan(&id, &name, &age, &email); err != nil {
				b.Fatal(err)
			}
			count++
		}
		rows.Close()
		if count != 10000 {
			b.Fatalf("expected 10000 rows, got %d", count)
		}
	}
}

func BenchmarkCmpSQLiteFileScan(b *testing.B) {
	db, cleanup := setupSQLiteFile(b)
	defer cleanup()
	preloadData(b, 10000, func(i int) error { return sqliteInsert(db, i) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`SELECT id, name, age, email FROM records`)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var id int
			var name, email string
			var age int
			if err := rows.Scan(&id, &name, &age, &email); err != nil {
				b.Fatal(err)
			}
			count++
		}
		rows.Close()
		if count != 10000 {
			b.Fatalf("expected 10000 rows, got %d", count)
		}
	}
}

func BenchmarkCmpEmbedDBMemScan(b *testing.B) {
	db, cleanup := setupEmbedDBMem(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 10000, func(i int) error {
		_, err := tbl.Insert(newCmpRecord(i))
		return err
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := tbl.Scan(func(r cmpRecord) bool {
			count++
			return true
		}); err != nil {
			b.Fatal(err)
		}
		if count != 10000 {
			b.Fatalf("expected 10000 scanned, got %d", count)
		}
	}
}

func BenchmarkCmpEmbedDBFileScan(b *testing.B) {
	db, cleanup := setupEmbedDBFile(b)
	defer cleanup()
	tbl, err := Use[cmpRecord](db, "records")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 10000, func(i int) error {
		_, err := tbl.Insert(newCmpRecord(i))
		return err
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := tbl.Scan(func(r cmpRecord) bool {
			count++
			return true
		}); err != nil {
			b.Fatal(err)
		}
		if count != 10000 {
			b.Fatalf("expected 10000 scanned, got %d", count)
		}
	}
}

// --- Mixed workload: inserts + reads intermixed ---

func BenchmarkCmpSQLiteMemMixed(b *testing.B) {
	db, cleanup := setupSQLiteMemIndexed(b)
	defer cleanup()
	preloadData(b, 5000, func(i int) error { return sqliteInsert(db, i) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			sqliteInsert(db, 5000+i)
		} else if i%3 == 1 {
			var name string
			db.QueryRow(`SELECT name FROM records WHERE id = ?`, (i%5000)+1).Scan(&name)
		} else {
			var age int
			db.QueryRow(`SELECT age FROM records WHERE name = ?`,
				fmt.Sprintf("user-%06d", (i%5000)+1)).Scan(&age)
		}
	}
}

func BenchmarkCmpEmbedDBFileMixed(b *testing.B) {
	db, cleanup := setupEmbedDBFile(b)
	defer cleanup()
	tbl, err := Use[cmpRecordIndexed](db, "records_m")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 5000, func(i int) error {
		_, err := tbl.Insert(newCmpRecordIndexed(i))
		return err
	})
	n := 5000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			n++
			tbl.Insert(newCmpRecordIndexed(n))
		} else if i%3 == 1 {
			tbl.Get(uint32((i % 5000) + 1))
		} else {
			tbl.Query("Name", fmt.Sprintf("user-%06d", (i%5000)+1))
		}
	}
}

func BenchmarkCmpEmbedDBMemMixed(b *testing.B) {
	db, cleanup := setupEmbedDBMem(b)
	defer cleanup()
	tbl, err := Use[cmpRecordIndexed](db, "records_m")
	if err != nil {
		b.Fatal(err)
	}
	preloadData(b, 5000, func(i int) error {
		_, err := tbl.Insert(newCmpRecordIndexed(i))
		return err
	})
	n := 5000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			n++
			tbl.Insert(newCmpRecordIndexed(n))
		} else if i%3 == 1 {
			tbl.Get(uint32((i % 5000) + 1))
		} else {
			tbl.Query("Name", fmt.Sprintf("user-%06d", (i%5000)+1))
		}
	}
}

// --- Memory after load ---

func TestCmpMemoryUsage(t *testing.T) {
	records := 10000
	data := strings.Repeat("x", 200)

	t.Run("embeddb", func(t *testing.T) {
		os.Remove("/tmp/cmp_mem_embed.db")
		db, err := Open("/tmp/cmp_mem_embed.db")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		defer os.Remove("/tmp/cmp_mem_embed.db")

		tbl, _ := Use[cmpRecordIndexed](db, "records")
		for i := 0; i < records; i++ {
			r := newCmpRecordIndexed(i)
			r.Email = data
			tbl.Insert(r)
		}
		stats := db.Stats()
		t.Logf("embeddb: %d records, file=%s, index_keys=%d, depth=%d, cache_hit=%.1f%%",
			stats.Tables["records"].RecordCount,
			fmt.Sprintf("%.1f MiB", float64(stats.FileSize)/(1024*1024)),
			stats.IndexKeys,
			stats.BTreeDepth,
			stats.CacheStats["primary"].HitRate*100,
		)
	})

	t.Run("sqlite", func(t *testing.T) {
		os.Remove("/tmp/cmp_mem_sqlite.db")
		sqlDB, err := sql.Open("sqlite3", "/tmp/cmp_mem_sqlite.db")
		if err != nil {
			t.Fatal(err)
		}
		defer sqlDB.Close()
		defer os.Remove("/tmp/cmp_mem_sqlite.db")

		sqlDB.Exec(`CREATE TABLE records (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, email TEXT)`)
		sqlDB.Exec(`CREATE INDEX idx_name ON records(name)`)

		tx, _ := sqlDB.Begin()
		stmt, _ := tx.Prepare(`INSERT INTO records (name, age, email) VALUES (?, ?, ?)`)
		for i := 0; i < records; i++ {
			stmt.Exec(fmt.Sprintf("user-%06d", i), 20+i%50, data)
		}
		stmt.Close()
		tx.Commit()

		var pageCount, freelistCount int
		sqlDB.QueryRow(`PRAGMA page_count`).Scan(&pageCount)
		sqlDB.QueryRow(`PRAGMA freelist_count`).Scan(&freelistCount)
		pageSize := 4096
		fileSize := int64((pageCount - freelistCount) * pageSize)
		t.Logf("sqlite: %d records, pages=%d, free=%d, est_file=%s",
			records, pageCount, freelistCount,
			fmt.Sprintf("%.1f MiB", float64(fileSize)/(1024*1024)))
	})
}
