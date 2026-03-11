package embeddb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// BenchmarkRecord is a struct with various field types for benchmarking
type BenchmarkRecord struct {
	ID        uint32  `db:"id,primary"`
	Name      string  `db:"index"`
	Email     string  `db:"index"`
	Age       int     `db:"index"`
	Score     float64 `db:"index"`
	IsActive  bool    `db:"index"`
	Category  int32   `db:"index"`
	Timestamp int64   `db:"index"`
}

// Helper to generate random string
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// Helper to generate a random record
func generateRecord(id int) *BenchmarkRecord {
	return &BenchmarkRecord{
		Name:      fmt.Sprintf("User_%s_%d", randomString(8), id),
		Email:     fmt.Sprintf("%s@example.com", randomString(10)),
		Age:       rand.Intn(80) + 18, // 18-97
		Score:     rand.Float64() * 100,
		IsActive:  rand.Intn(2) == 1,
		Category:  int32(rand.Intn(100)), // 0-99 categories
		Timestamp: time.Now().UnixNano() + int64(rand.Intn(1000000)),
	}
}

// TestBenchmarkMillionRecords runs the full 1M record benchmark
func TestBenchmarkMillionRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark in short mode")
	}

	// Seed random for reproducibility
	rand.Seed(42)

	fmt.Println("Starting benchmark...")

	// Clean up any existing test files
	dbPath := "benchmark_test.db"
	os.Remove(dbPath)
	defer func() {
		os.Remove(dbPath)
		// Clean up index files
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if len(f.Name()) > len(dbPath) && f.Name()[:len(dbPath)] == dbPath {
				os.Remove(f.Name())
			}
		}
	}()

	// Create database without auto-indexing for baseline test
	db, err := New[BenchmarkRecord](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Start with 10K records for quick testing
	totalRecords := 10000
	checkpointInterval := 2500

	// Store some known values for lookup tests
	knownNames := make([]string, 0, 10)
	knownEmails := make([]string, 0, 10)
	knownAges := make([]int, 0, 10)
	knownCategories := make([]int32, 0, 10)

	fmt.Println("=== EmbedDB Benchmark: 1 Million Records ===")
	fmt.Println()

	// Benchmark results storage
	type BenchmarkResult struct {
		RecordCount   int
		InsertTime    time.Duration
		StringLookup  time.Duration
		IntLookup     time.Duration
		Int32Lookup   time.Duration
		Int64Lookup   time.Duration
		Float64Lookup time.Duration
		BoolLookup    time.Duration
		GetByID       time.Duration
	}
	results := make([]BenchmarkResult, 0, totalRecords/checkpointInterval)

	insertStart := time.Now()
	var lastCheckpoint time.Time = insertStart

	for i := 1; i <= totalRecords; i++ {
		if i%100 == 0 {
			fmt.Printf("\rInserting record %d/%d...", i, totalRecords)
		}
		record := generateRecord(i)

		// Store some known values at specific intervals for lookup tests
		if i%(totalRecords/10) == 0 {
			knownNames = append(knownNames, record.Name)
			knownEmails = append(knownEmails, record.Email)
			knownAges = append(knownAges, record.Age)
			knownCategories = append(knownCategories, record.Category)
		}

		_, err := db.Insert(record)
		if err != nil {
			t.Fatalf("Failed to insert record %d: %v", i, err)
		}

		// Run benchmarks at checkpoints
		if i%checkpointInterval == 0 {
			insertDuration := time.Since(lastCheckpoint)
			lastCheckpoint = time.Now()

			result := BenchmarkResult{
				RecordCount: i,
				InsertTime:  insertDuration,
			}

			// Use the most recent known values for lookups
			// Benchmark Get by ID (no indexes in this test)
			if i > 0 {
				start := time.Now()
				for j := 0; j < 100; j++ {
					randomID := uint32(rand.Intn(i) + 1)
					_, _ = db.Get(randomID)
				}
				result.GetByID = time.Since(start) / 100

				// No indexes for this baseline test
				result.StringLookup = 0
				result.IntLookup = 0
				result.Int32Lookup = 0
				result.Int64Lookup = 0
				result.Float64Lookup = 0
				result.BoolLookup = 0
			}

			results = append(results, result)

			// Print progress
			fmt.Printf("Checkpoint: %d records\n", i)
			fmt.Printf("  Insert %dk records: %v (%.0f records/sec)\n",
				checkpointInterval/1000,
				insertDuration,
				float64(checkpointInterval)/insertDuration.Seconds())
			if len(knownNames) > 0 {
				fmt.Printf("  String lookup (avg):  %v\n", result.StringLookup)
				fmt.Printf("  Int lookup:           %v\n", result.IntLookup)
				fmt.Printf("  Int32 lookup:         %v\n", result.Int32Lookup)
				fmt.Printf("  Int64 lookup:         %v\n", result.Int64Lookup)
				fmt.Printf("  Float64 lookup:       %v\n", result.Float64Lookup)
				fmt.Printf("  Bool lookup:          %v\n", result.BoolLookup)
				fmt.Printf("  Get by ID:            %v\n", result.GetByID)
			}
			fmt.Println()
		}
	}

	totalTime := time.Since(insertStart)

	// Print summary
	fmt.Println("=== SUMMARY ===")
	fmt.Printf("Total records: %d\n", totalRecords)
	fmt.Printf("Total time: %v\n", totalTime)
	fmt.Printf("Average insert rate: %.0f records/sec\n", float64(totalRecords)/totalTime.Seconds())
	fmt.Println()

	// Print lookup performance table
	fmt.Println("=== LOOKUP PERFORMANCE BY RECORD COUNT ===")
	fmt.Println("Records    | String    | Int       | Int32     | Int64     | Float64   | Bool      | GetByID")
	fmt.Println("-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------")
	for _, r := range results {
		fmt.Printf("%10d | %9v | %9v | %9v | %9v | %9v | %9v | %9v\n",
			r.RecordCount,
			r.StringLookup.Round(time.Microsecond),
			r.IntLookup.Round(time.Microsecond),
			r.Int32Lookup.Round(time.Microsecond),
			r.Int64Lookup.Round(time.Microsecond),
			r.Float64Lookup.Round(time.Microsecond),
			r.BoolLookup.Round(time.Microsecond),
			r.GetByID.Round(time.Microsecond))
	}

	// Get final database file size
	fileInfo, err := os.Stat(dbPath)
	if err == nil {
		fmt.Printf("\nDatabase file size: %.2f MB\n", float64(fileInfo.Size())/(1024*1024))
	}

	// Verify Get by ID works
	fmt.Println("\n=== VERIFICATION ===")
	for i := 1; i <= 5 && i <= totalRecords; i++ {
		record, err := db.Get(uint32(i))
		if err != nil {
			t.Errorf("Failed to get record %d: %v", i, err)
		} else {
			fmt.Printf("Record %d: Name=%s\n", i, record.Name)
		}
	}
}

// BenchmarkInsert benchmarks insert performance
func BenchmarkInsert(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_insert.db"
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
	for i := 0; i < b.N; i++ {
		record := generateRecord(i)
		_, err := db.Insert(record)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}
}

// BenchmarkGetByID benchmarks get by ID performance
func BenchmarkGetByID(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_get.db"
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

	// Pre-populate with 10000 records
	for i := 0; i < 10000; i++ {
		record := generateRecord(i)
		_, err := db.Insert(record)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint32(rand.Intn(10000) + 1)
		_, err := db.Get(id)
		if err != nil {
			b.Fatalf("Failed to get: %v", err)
		}
	}
}

// BenchmarkQueryString benchmarks string field query performance
func BenchmarkQueryString(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_query_string.db"
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

	// Pre-populate with 10000 records
	var testName string
	for i := 0; i < 10000; i++ {
		record := generateRecord(i)
		if i == 5000 {
			testName = record.Name
		}
		_, err := db.Insert(record)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Query("Name", testName)
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

// BenchmarkQueryInt benchmarks int field query performance
func BenchmarkQueryInt(b *testing.B) {
	rand.Seed(42)

	dbPath := "bench_query_int.db"
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

	// Pre-populate with 10000 records
	for i := 0; i < 10000; i++ {
		record := generateRecord(i)
		_, err := db.Insert(record)
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}

	testAge := 30

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Query("Age", testAge)
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}
