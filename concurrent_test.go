package embeddb

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPerfTableConcurrentGoroutines(t *testing.T) {
	removeFile := func(name string) { os.Remove(name) }
	fn := "/tmp/perf_test_goroutines.db"
	removeFile(fn)
	defer removeFile(fn)

	db, err := New[PerfUser](fn, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		t.Fatal(err)
	}

	users.CreateIndex("Age")
	users.CreateIndex("City")

	const numWriters = 4
	const recordsPerWriter = 25000

	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			base := writerID * recordsPerWriter
			for i := 0; i < recordsPerWriter; i++ {
				_, err := users.Insert(&PerfUser{
					Name:     fmt.Sprintf("User_%d", base+i),
					Age:      20 + ((base + i) % 80),
					City:     fmt.Sprintf("City_%d", (base+i)%50),
					Category: fmt.Sprintf("Cat_%d", (base+i)%20),
					Status:   []string{"active", "inactive", "pending"}[(base+i)%3],
				})
				if err != nil {
					t.Errorf("Insert error: %v", err)
				}
			}
		}(w)
	}

	wg.Wait()
	totalDur := time.Since(start)

	totalRecords := numWriters * recordsPerWriter
	fmt.Printf("Concurrent: %d writers, %d records in %.2fs (%.0f rec/sec)\n",
		numWriters, totalRecords, totalDur.Seconds(), float64(totalRecords)/totalDur.Seconds())

	count := users.Count()
	fmt.Printf("Final count: %d\n", count)
	if count != totalRecords {
		t.Errorf("Expected %d, got %d", totalRecords, count)
	}
}
