package embeddb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func cleanupDeleteBenchFiles(dbPath string) {
	_ = os.Remove(dbPath)
	files, _ := filepath.Glob(dbPath + ".*.idx")
	for _, f := range files {
		_ = os.Remove(f)
	}
}

func prefillDeleteBench(b *testing.B, table *Table[PerfUser], n int) []uint32 {
	b.Helper()
	ids := make([]uint32, 0, n)
	for i := 0; i < n; i++ {
		id, err := table.Insert(&PerfUser{
			Name:     fmt.Sprintf("User_%d", i),
			Age:      20 + (i % 80),
			City:     fmt.Sprintf("City_%d", i%50),
			Category: fmt.Sprintf("Cat_%d", i%20),
			Status:   []string{"active", "inactive", "pending"}[i%3],
		})
		if err != nil {
			b.Fatalf("prefill insert failed at %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func BenchmarkDeleteNoSecondaryIndexes(b *testing.B) {
	dbPath := "/tmp/delete_bench_noidx.db"
	cleanupDeleteBenchFiles(dbPath)
	defer cleanupDeleteBenchFiles(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		b.Fatal(err)
	}

	ids := prefillDeleteBench(b, users, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := users.Delete(ids[i]); err != nil {
			b.Fatalf("delete failed at %d: %v", i, err)
		}
	}
}

func BenchmarkDeleteWithThreeSecondaryIndexes(b *testing.B) {
	dbPath := "/tmp/delete_bench_3idx.db"
	cleanupDeleteBenchFiles(dbPath)
	defer cleanupDeleteBenchFiles(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		b.Fatal(err)
	}

	if err := users.CreateIndex("Age"); err != nil {
		b.Fatal(err)
	}
	if err := users.CreateIndex("City"); err != nil {
		b.Fatal(err)
	}
	if err := users.CreateIndex("Status"); err != nil {
		b.Fatal(err)
	}

	ids := prefillDeleteBench(b, users, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := users.Delete(ids[i]); err != nil {
			b.Fatalf("delete failed at %d: %v", i, err)
		}
	}
}
