package embeddb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type PerfUser struct {
	Name     string
	Age      int    `db:"index"`
	City     string `db:"index"`
	Category string
	Status   string `db:"index"`
}

func TestPerfTableOperations(t *testing.T) {
	removeFile := func(name string) { os.Remove(name) }

	t.Run("100 entries", func(t *testing.T) {
		fn := "/tmp/perf_test_100.db"
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

		start := time.Now()
		for i := 0; i < 100; i++ {
			_, err := users.Insert(&PerfUser{
				Name:     fmt.Sprintf("User_%d", i),
				Age:      20 + (i % 50),
				City:     fmt.Sprintf("City_%d", i%10),
				Category: fmt.Sprintf("Cat_%d", i%5),
				Status:   []string{"active", "inactive"}[i%2],
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		insertDur := time.Since(start)

		// Query
		start = time.Now()
		for i := 0; i < 10; i++ {
			users.Query("Age", 25)
		}
		queryDur := time.Since(start)

		// Filter
		start = time.Now()
		users.Filter(func(u PerfUser) bool { return u.Age > 30 })
		filterDur := time.Since(start)

		// Scan
		start = time.Now()
		count := 0
		users.Scan(func(u PerfUser) bool { count++; return true })
		scanDur := time.Since(start)

		fmt.Printf("100 entries: insert=%v, query=%v, filter=%v, scan=%v\n",
			insertDur, queryDur, filterDur, scanDur)
	})

	t.Run("1000 entries", func(t *testing.T) {
		fn := "/tmp/perf_test_1k.db"
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

		start := time.Now()
		for i := 0; i < 1000; i++ {
			_, err := users.Insert(&PerfUser{
				Name:     fmt.Sprintf("User_%d", i),
				Age:      20 + (i % 50),
				City:     fmt.Sprintf("City_%d", i%10),
				Category: fmt.Sprintf("Cat_%d", i%5),
				Status:   []string{"active", "inactive"}[i%2],
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		insertDur := time.Since(start)

		start = time.Now()
		for i := 0; i < 10; i++ {
			users.Query("Age", 25)
		}
		queryDur := time.Since(start)

		start = time.Now()
		users.Filter(func(u PerfUser) bool { return u.Age > 30 })
		filterDur := time.Since(start)

		start = time.Now()
		count := 0
		users.Scan(func(u PerfUser) bool { count++; return true })
		scanDur := time.Since(start)

		fmt.Printf("1k entries: insert=%.2fs, query=%v, filter=%v, scan=%v\n",
			insertDur.Seconds(), queryDur, filterDur, scanDur)
	})

	t.Run("10000 entries", func(t *testing.T) {
		fn := "/tmp/perf_test_10k.db"
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

		start := time.Now()
		for i := 0; i < 10000; i++ {
			_, err := users.Insert(&PerfUser{
				Name:     fmt.Sprintf("User_%d", i),
				Age:      20 + (i % 50),
				City:     fmt.Sprintf("City_%d", i%10),
				Category: fmt.Sprintf("Cat_%d", i%5),
				Status:   []string{"active", "inactive"}[i%2],
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		insertDur := time.Since(start)

		start = time.Now()
		for i := 0; i < 10; i++ {
			users.Query("Age", 25)
		}
		queryDur := time.Since(start)

		start = time.Now()
		users.Filter(func(u PerfUser) bool { return u.Age > 30 })
		filterDur := time.Since(start)

		start = time.Now()
		count := 0
		users.Scan(func(u PerfUser) bool { count++; return true })
		scanDur := time.Since(start)

		fmt.Printf("10k entries: insert=%.2fs, query=%v, filter=%v, scan=%v\n",
			insertDur.Seconds(), queryDur, filterDur, scanDur)
	})
}

func TestPerfTable100K(t *testing.T) {
	removeFile := func(name string) { os.Remove(name) }
	fn := "/tmp/perf_test_100k.db"
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

	// Create indexes
	if err := users.CreateIndex("Age"); err != nil {
		t.Fatal(err)
	}
	if err := users.CreateIndex("City"); err != nil {
		t.Fatal(err)
	}
	if err := users.CreateIndex("Status"); err != nil {
		t.Fatal(err)
	}

	t.Run("100k entries", func(t *testing.T) {
		start := time.Now()

		for i := 0; i < 100000; i++ {
			_, err := users.Insert(&PerfUser{
				Name:     fmt.Sprintf("User_%d", i),
				Age:      20 + (i % 80),
				City:     fmt.Sprintf("City_%d", i%50),
				Category: fmt.Sprintf("Cat_%d", i%20),
				Status:   []string{"active", "inactive", "pending"}[i%3],
			})
			if err != nil {
				t.Fatal(err)
			}

			if (i+1)%25000 == 0 {
				fmt.Printf("Inserted %d records...\n", i+1)
			}
		}

		insertDur := time.Since(start)
		fmt.Printf("100k insert: %.2fs (%.0f rec/sec)\n",
			insertDur.Seconds(), 100000/insertDur.Seconds())

		// Query by Age
		start = time.Now()
		for i := 0; i < 100; i++ {
			users.Query("Age", 50)
		}
		ageQueryDur := time.Since(start)
		fmt.Printf("100 Age queries: %v (%.2fms/query)\n",
			ageQueryDur, float64(ageQueryDur.Milliseconds())/100)

		// Query by City
		start = time.Now()
		for i := 0; i < 100; i++ {
			users.Query("City", "City_25")
		}
		cityQueryDur := time.Since(start)
		fmt.Printf("100 City queries: %v (%.2fms/query)\n",
			cityQueryDur, float64(cityQueryDur.Milliseconds())/100)

		// Query by Status
		start = time.Now()
		for i := 0; i < 100; i++ {
			users.Query("Status", "active")
		}
		statusQueryDur := time.Since(start)
		fmt.Printf("100 Status queries: %v (%.2fms/query)\n",
			statusQueryDur, float64(statusQueryDur.Milliseconds())/100)

		// Filter
		start = time.Now()
		users.Filter(func(u PerfUser) bool { return u.Age > 50 && u.City == "City_10" })
		filterDur := time.Since(start)
		fmt.Printf("Filter: %v\n", filterDur)

		// Scan with count
		start = time.Now()
		count := 0
		users.Scan(func(u PerfUser) bool { count++; return true })
		scanDur := time.Since(start)
		fmt.Printf("Scan: %v (count=%d)\n", scanDur, count)

		// Verify count
		if count != 100000 {
			t.Errorf("Expected 100000 records, got %d", count)
		}
	})
}

func TestPerfTableConcurrent(t *testing.T) {
	removeFile := func(name string) { os.Remove(name) }
	fn := "/tmp/perf_test_concurrent.db"
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

	// Create indexes
	users.CreateIndex("Age")
	users.CreateIndex("City")

	t.Run("100k with concurrent patterns", func(t *testing.T) {
		start := time.Now()

		// Insert
		for i := 0; i < 100000; i++ {
			_, err := users.Insert(&PerfUser{
				Name:     fmt.Sprintf("User_%d", i),
				Age:      20 + (i % 80),
				City:     fmt.Sprintf("City_%d", i%50),
				Category: fmt.Sprintf("Cat_%d", i%20),
				Status:   []string{"active", "inactive", "pending"}[i%3],
			})
			if err != nil {
				t.Fatal(err)
			}

			if (i+1)%25000 == 0 {
				fmt.Printf("Inserted %d records...\n", i+1)
			}
		}
		insertDur := time.Since(start)
		fmt.Printf("100k insert: %.2fs (%.0f rec/sec)\n",
			insertDur.Seconds(), 100000/insertDur.Seconds())

		// Mixed operations
		start = time.Now()
		for i := 0; i < 1000; i++ {
			users.Query("Age", i%80)
			users.Query("City", fmt.Sprintf("City_%d", i%50))
			users.Filter(func(u PerfUser) bool { return u.Age > 50 })
		}
		mixedDur := time.Since(start)
		fmt.Printf("1000 mixed ops (2 queries + 1 filter): %v\n", mixedDur)

		// Full scan
		start = time.Now()
		count := 0
		users.Scan(func(u PerfUser) bool { count++; return true })
		scanDur := time.Since(start)
		fmt.Printf("Full scan: %v (count=%d)\n", scanDur, count)

		if count != 100000 {
			t.Errorf("Expected 100000, got %d", count)
		}
	})
}

func TestPerfTableReopen(t *testing.T) {
	removeFile := func(name string) { os.Remove(name) }
	fn := "/tmp/perf_test_reopen.db"
	removeFile(fn)
	defer removeFile(fn)

	// Insert data
	{
		db, err := New[PerfUser](fn, false, false)
		if err != nil {
			t.Fatal(err)
		}

		users, err := db.Table()
		if err != nil {
			t.Fatal(err)
		}

		users.CreateIndex("Age")
		users.CreateIndex("City")

		for i := 0; i < 50000; i++ {
			users.Insert(&PerfUser{
				Name:     fmt.Sprintf("User_%d", i),
				Age:      20 + (i % 80),
				City:     fmt.Sprintf("City_%d", i%50),
				Category: fmt.Sprintf("Cat_%d", i%20),
				Status:   []string{"active", "inactive", "pending"}[i%3],
			})
			if (i+1)%25000 == 0 {
				fmt.Printf("Inserted %d records...\n", i+1)
			}
		}
		db.Close()
		fmt.Println("First DB closed")
	}

	// Reopen and query
	{
		db, err := New[PerfUser](fn, false, false)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		users, err := db.Table()
		if err != nil {
			t.Fatal(err)
		}

		// Query
		start := time.Now()
		for i := 0; i < 100; i++ {
			users.Query("Age", 50)
		}
		queryDur := time.Since(start)
		fmt.Printf("100 queries after reopen: %v\n", queryDur)

		// Count
		count := 0
		users.Scan(func(u PerfUser) bool { count++; return true })
		fmt.Printf("Count after reopen: %d\n", count)

		if count != 50000 {
			t.Errorf("Expected 50000, got %d", count)
		}
	}
}
