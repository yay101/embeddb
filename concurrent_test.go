package embeddb

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestConcurrentAccess(t *testing.T) {
	os.Remove("/tmp/concurrent.db")
	defer os.Remove("/tmp/concurrent.db")

	db, _ := Open("/tmp/concurrent.db")
	users, _ := Use[User](db, "users")

	var wg sync.WaitGroup
	numWriters := 4
	numReaders := 8
	insertsPerWriter := 100

	start := time.Now()

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < insertsPerWriter; i++ {
				users.Insert(&User{
					Name:  fmt.Sprintf("User%d_%d", id, i),
					Email: fmt.Sprintf("user%d_%d@test.com", id, i),
					Age:   20 + (id*10+i)%50,
				})
			}
		}(w)
	}

	// Readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				users.Count()
				users.All()
				users.Filter(func(u User) bool { return u.Age > 30 })
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	count := users.Count()
	expected := numWriters * insertsPerWriter
	if count != expected {
		t.Errorf("expected %d records, got %d", expected, count)
	}

	db.Close()

	totalOps := (numWriters * insertsPerWriter) + (numReaders * 50 * 3)
	fmt.Printf("\n[TestConcurrentAccess] %d ops in %v (%.0f ops/sec)\n",
		totalOps, elapsed, float64(totalOps)/elapsed.Seconds())
	fmt.Printf("  Writers: %d x %d inserts = %d records\n",
		numWriters, insertsPerWriter, numWriters*insertsPerWriter)
	fmt.Printf("  Readers: %d x %d x 3 ops = %d reads\n",
		numReaders, 50, numReaders*50*3)
}

func TestConcurrentWrites(t *testing.T) {
	os.Remove("/tmp/concurrent_writes.db")
	defer os.Remove("/tmp/concurrent_writes.db")

	db, _ := Open("/tmp/concurrent_writes.db")
	users, _ := Use[User](db, "users")

	var wg sync.WaitGroup
	numWriters := 8
	insertsPerWriter := 250

	start := time.Now()

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < insertsPerWriter; i++ {
				users.Insert(&User{
					Name: fmt.Sprintf("Writer%d_Item%d", id, i),
					Age:  25,
				})
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	count := users.Count()
	expected := numWriters * insertsPerWriter
	if count != expected {
		t.Errorf("expected %d records, got %d", expected, count)
	}

	db.Close()

	totalInserts := numWriters * insertsPerWriter
	fmt.Printf("\n[TestConcurrentWrites] %d inserts in %v (%.0f inserts/sec)\n",
		totalInserts, elapsed, float64(totalInserts)/elapsed.Seconds())
}

func TestConcurrentMixedOps(t *testing.T) {
	os.Remove("/tmp/concurrent_mixed.db")
	defer os.Remove("/tmp/concurrent_mixed.db")

	db, _ := Open("/tmp/concurrent_mixed.db")
	users, _ := Use[User](db, "users")

	// Pre-populate
	for i := 0; i < 500; i++ {
		users.Insert(&User{Name: fmt.Sprintf("Initial%d", i), Age: i % 50})
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				op := (id + i) % 5
				switch op {
				case 0:
					users.Insert(&User{Name: fmt.Sprintf("Goroutine%d_Insert%d", id, i), Age: 30})
				case 1:
					users.Count()
				case 2:
					users.All()
				case 3:
					users.Filter(func(u User) bool { return u.Age > 25 })
				case 4:
					s := users.ScanRecords()
					s.Close()
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	db.Close()

	totalOps := numGoroutines * opsPerGoroutine
	fmt.Printf("\n[TestConcurrentMixedOps] %d ops in %v (%.0f ops/sec)\n",
		totalOps, elapsed, float64(totalOps)/elapsed.Seconds())
}

func BenchmarkConcurrentWrites(b *testing.B) {
	os.Remove("/tmp/bench_concurrent.db")
	defer os.Remove("/tmp/bench_concurrent.db")

	db, _ := Open("/tmp/bench_concurrent.db")
	users, _ := Use[User](db, "users")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		users.Insert(&User{Name: fmt.Sprintf("User%d", i), Age: 25})
	}

	b.StopTimer()
	db.Close()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "inserts/sec")
}

func BenchmarkConcurrentReads(b *testing.B) {
	os.Remove("/tmp/bench_concurrent_reads.db")
	defer os.Remove("/tmp/bench_concurrent_reads.db")

	db, _ := Open("/tmp/bench_concurrent_reads.db")
	users, _ := Use[User](db, "users")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		users.Insert(&User{Name: fmt.Sprintf("User%d", i), Age: i % 50})
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			users.Count()
		}()
	}
	wg.Wait()

	b.StopTimer()
	db.Close()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
}
