package embeddb

// Benchmark Notes:
// These benchmarks run on a single AMD Milan core with 1GB RAM.
// Results will vary based on hardware.

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

func BenchmarkFilterAll(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_filter.db"
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
		db.Insert(record)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Filter(func(r BenchmarkRecord) bool {
			return r.Age > 0
		})
		if err != nil {
			b.Fatalf("Filter failed: %v", err)
		}
	}
}

func BenchmarkFilterMatch(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_filter_match.db"
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
		db.Insert(record)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Filter(func(r BenchmarkRecord) bool {
			return r.Age > 50 // Matches ~30% of records
		})
		if err != nil {
			b.Fatalf("Filter failed: %v", err)
		}
	}
}

func BenchmarkScan(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_scan.db"
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
		db.Insert(record)
	}

	b.ResetTimer()
	b.ReportAllocs()

	count := 0
	for i := 0; i < b.N; i++ {
		err := db.Scan(func(r BenchmarkRecord) bool {
			count++
			return true
		})
		if err != nil {
			b.Fatalf("Scan failed: %v", err)
		}
	}
}

func BenchmarkScanEarlyExit(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_scan_early.db"
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
		db.Insert(record)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Scan but stop after 100 records
	for i := 0; i < b.N; i++ {
		count := 0
		err := db.Scan(func(r BenchmarkRecord) bool {
			count++
			return count < 100 // Early exit after 100 records
		})
		if err != nil {
			b.Fatalf("Scan failed: %v", err)
		}
	}
}

func BenchmarkFilterNested(b *testing.B) {
	rand.Seed(42)

	type TestRecordNested struct {
		ID     uint32
		Name   string
		Age    int
		Nested struct {
			City string
			Zip  int
		}
	}

	dbPath := "bench_filter_nested.db"
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

	db, err := New[TestRecordNested](dbPath, false, false)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		db.Insert(&TestRecordNested{
			ID:   uint32(i),
			Name: fmt.Sprintf("User%d", i),
			Age:  rand.Intn(80) + 18,
		})
		// Note: Nested field not set for simplicity
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Filter(func(r TestRecordNested) bool {
			return r.Age > 50
		})
		if err != nil {
			b.Fatalf("Filter failed: %v", err)
		}
	}
}

func TestUnindexedScanBenchmarks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark test in short mode")
	}

	rand.Seed(42)

	dbPath := "bench_unindexed.db"
	os.Remove(dbPath)
	defer cleanupTestFiles(dbPath)

	db, err := New[BenchmarkRecord](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Pre-populate with 10000 records
	for i := 0; i < 10000; i++ {
		db.Insert(generateRecord(i))
	}

	fmt.Println("\n=== Unindexed Scan Benchmarks (10,000 records) ===")
	fmt.Println("System: AMD Milan (1 core, 1GB RAM)")
	fmt.Println()

	// Benchmark Filter - match all
	start := time.Now()
	count := 0
	db.Scan(func(r BenchmarkRecord) bool { count++; return true })
	elapsed := time.Since(start)
	fmt.Printf("Full scan (iterating all):    %8.2f ms (%d records)\n", float64(elapsed.Microseconds())/1000, count)

	// Benchmark Filter - match some
	start = time.Now()
	results, _ := db.Filter(func(r BenchmarkRecord) bool { return r.Age > 50 })
	elapsed = time.Since(start)
	fmt.Printf("Filter (age > 50):             %8.2f ms (%d results)\n", float64(elapsed.Microseconds())/1000, len(results))

	// Benchmark Scan with early exit
	start = time.Now()
	earlyCount := 0
	db.Scan(func(r BenchmarkRecord) bool {
		earlyCount++
		return earlyCount < 100 // Stop after 100
	})
	elapsed = time.Since(start)
	fmt.Printf("Scan (early exit at 100):      %8.2f ms (%d records)\n", float64(elapsed.Microseconds())/1000, earlyCount)

	// Get memory stats
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	fmt.Printf("\nHeap memory:        %s\n", formatBytes(stats.HeapAlloc))
	fmt.Printf("Records/second:     %.0f\n", 10000/elapsed.Seconds())
	fmt.Println()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestPeakMemoryDuringOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	rand.Seed(42)
	dbPath := "test_peak_mem.db"
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
		t.Fatalf("Failed to create db: %v", err)
	}
	defer db.Close()

	var peakHeap, peakSys uint64
	var m runtime.MemStats

	// Measure peak memory during insert
	for i := 0; i < 10000; i++ {
		record := generateRecord(i)
		_, err := db.Insert(record)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		if i%100 == 0 {
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peakHeap {
				peakHeap = m.HeapAlloc
			}
			if m.Sys > peakSys {
				peakSys = m.Sys
			}
		}
	}
	runtime.ReadMemStats(&m)
	if m.HeapAlloc > peakHeap {
		peakHeap = m.HeapAlloc
	}
	if m.Sys > peakSys {
		peakSys = m.Sys
	}

	fmt.Printf("\n=== Peak Memory During 10k Inserts ===\n")
	fmt.Printf("Peak Heap: %s\n", formatBytes(peakHeap))
	fmt.Printf("Peak Sys:  %s\n", formatBytes(peakSys))

	// Measure peak memory during queries
	peakHeap, peakSys = 0, 0
	for i := 0; i < 5000; i++ {
		_, _ = db.Query("Age", 50)
		if i%100 == 0 {
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peakHeap {
				peakHeap = m.HeapAlloc
			}
			if m.Sys > peakSys {
				peakSys = m.Sys
			}
		}
	}
	runtime.ReadMemStats(&m)
	if m.HeapAlloc > peakHeap {
		peakHeap = m.HeapAlloc
	}
	if m.Sys > peakSys {
		peakSys = m.Sys
	}

	fmt.Printf("\n=== Peak Memory During 5k Queries ===\n")
	fmt.Printf("Peak Heap: %s\n", formatBytes(peakHeap))
	fmt.Printf("Peak Sys:  %s\n", formatBytes(peakSys))
}
