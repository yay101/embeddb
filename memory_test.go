package embeddb

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

func getMemoryStats() runtime.MemStats {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats
}

func formatBytes(bytes uint64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%.2f GB", float64(bytes)/(1024*1024*1024))
}

func TestMemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory benchmark in short mode")
	}

	rand.Seed(42)

	dbPath := "test_memory.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	recordCounts := []int{1000, 5000, 10000, 50000}

	fmt.Println("=== Memory Usage Benchmark ===")
	fmt.Println()

	for _, count := range recordCounts {
		// Create fresh database
		db, err := New[BenchmarkRecord](dbPath, false, false)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		// Insert records
		for i := 0; i < count; i++ {
			record := generateRecord(i)
			_, err := db.Insert(record)
			if err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		// Force GC and get memory stats
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)

		// Get file size
		fileInfo, _ := os.Stat(dbPath)

		fmt.Printf("Records: %d\n", count)
		fmt.Printf("  File size:       %s\n", formatBytes(uint64(fileInfo.Size())))
		fmt.Printf("  Heap alloc:      %s\n", formatBytes(stats.HeapAlloc))
		fmt.Printf("  Total alloc:     %s\n", formatBytes(stats.TotalAlloc))
		fmt.Printf("  Sys:             %s\n", formatBytes(stats.Sys))
		fmt.Printf("  NumGC:           %d\n", stats.NumGC)
		fmt.Println()

		db.Close()
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}
}

func TestMemoryUsageWithIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory benchmark in short mode")
	}

	rand.Seed(42)

	dbPath := "test_memory_idx.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	// Create database with auto-indexing
	db, err := New[BenchmarkRecord](dbPath, false, true)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	recordCounts := []int{1000, 5000, 10000}

	fmt.Println("=== Memory Usage With Indexes ===")
	fmt.Println()

	for _, count := range recordCounts {
		// Insert records (indexes are built automatically)
		for i := 0; i < count; i++ {
			record := generateRecord(i)
			_, err := db.Insert(record)
			if err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		// Force GC and get memory stats
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)

		// Get total file size (db + indexes)
		totalFileSize := uint64(0)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath)-3 && f.Name()[:len(dbPath)-3] == dbPath[:len(dbPath)-3] {
				fi, _ := f.Info()
				totalFileSize += uint64(fi.Size())
			}
		}

		fmt.Printf("Records: %d\n", count)
		fmt.Printf("  Total file size: %s\n", formatBytes(totalFileSize))
		fmt.Printf("  Heap alloc:      %s\n", formatBytes(stats.HeapAlloc))
		fmt.Printf("  Total alloc:     %s\n", formatBytes(stats.TotalAlloc))
		fmt.Printf("  Sys:             %s\n", formatBytes(stats.Sys))
		fmt.Println()
	}

	db.Close()
}

func BenchmarkMemoryInsert(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_mem_insert.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	db, err := New[BenchmarkRecord](dbPath, false, false)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	var stats runtime.MemStats

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		record := generateRecord(i)
		_, err := db.Insert(record)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}

	runtime.ReadMemStats(&stats)
	b.ReportMetric(float64(stats.HeapAlloc), "bytes/ops(heap)")
	b.ReportMetric(float64(stats.TotalAlloc), "bytes/ops(total)")
}

func BenchmarkMemoryInsertWithIndex(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_mem_insert_idx.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	db, err := New[BenchmarkRecord](dbPath, false, true)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		record := generateRecord(i)
		_, err := db.Insert(record)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}
}

func BenchmarkMemoryGet(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_mem_get.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	db, err := New[BenchmarkRecord](dbPath, false, false)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		record := generateRecord(i)
		_, err := db.Insert(record)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}

	var stats runtime.MemStats

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := uint32(rand.Intn(10000) + 1)
		_, err := db.Get(id)
		if err != nil {
			b.Fatalf("Failed to get: %v", err)
		}
	}

	runtime.ReadMemStats(&stats)
	b.ReportMetric(float64(stats.HeapAlloc), "bytes/ops(heap)")
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
