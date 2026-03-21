package embeddb

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type Company struct {
	ID        uint32 `db:"id,primary"`
	Name      string
	City      string `db:"index"`
	Employees int    `db:"index"`
}

type Order struct {
	ID         uint32    `db:"id,primary"`
	CustomerID uint32    `db:"index"`
	Product    string    `db:"index"`
	Quantity   int       `db:"index"`
	Price      float64   `db:"index"`
	CreatedAt  time.Time `db:"index"`
	Status     string    `db:"index"`
}

type EmployeeWithNested struct {
	ID         uint32 `db:"id,primary"`
	Name       string `db:"index"`
	Department Department
	Manager    Manager
	Salary     int       `db:"index"`
	HireDate   time.Time `db:"index"`
}

type Department struct {
	Name   string `db:"index"`
	Code   string `db:"index"`
	Office Office
}

type Office struct {
	City     string `db:"index"`
	Floor    int    `db:"index"`
	Building string
}

type Manager struct {
	Name  string `db:"index"`
	Title string `db:"index"`
}

func TestLargeDataset_SingleTable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large-record test in short mode")
	}

	dbPath := "/tmp/test_large_single.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	users.CreateIndex("Age")
	users.CreateIndex("City")
	users.CreateIndex("Status")

	const targetRecords = 200_000
	batchSize := 10000
	start := time.Now()

	for batch := 0; batch < targetRecords/batchSize; batch++ {
		for i := 0; i < batchSize; i++ {
			idx := batch*batchSize + i
			_, err := users.Insert(&PerfUser{
				Name:     fmt.Sprintf("User_%d", idx),
				Age:      20 + (idx % 80),
				City:     fmt.Sprintf("City_%d", idx%100),
				Category: fmt.Sprintf("Cat_%d", idx%50),
				Status:   []string{"active", "inactive", "pending"}[idx%3],
			})
			if err != nil {
				t.Fatalf("Insert failed at %d: %v", idx, err)
			}
		}

		if (batch+1)%5 == 0 {
			fmt.Printf("Inserted %d / %d records (%.1f%%)\n",
				(batch+1)*batchSize, targetRecords,
				float64((batch+1)*batchSize)/float64(targetRecords)*100)
		}
	}

	insertDur := time.Since(start)
	fmt.Printf("\n=== Large Dataset Insert ===\n")
	fmt.Printf("Total records: %d\n", targetRecords)
	fmt.Printf("Time: %.2fs\n", insertDur.Seconds())
	fmt.Printf("Rate: %.0f rec/sec\n", float64(targetRecords)/insertDur.Seconds())

	count := users.Count()
	if count != targetRecords {
		t.Errorf("Expected %d records, got %d", targetRecords, count)
	}

	start = time.Now()
	for i := 0; i < 100; i++ {
		results, err := users.Query("Age", 50)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("Expected results for Age=50")
		}
	}
	queryDur := time.Since(start)
	fmt.Printf("100 Age queries: %v (%.2fms/query)\n", queryDur, float64(queryDur.Milliseconds())/100)

	start = time.Now()
	for i := 0; i < 100; i++ {
		results, err := users.Query("City", "City_50")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("Expected results for City_50")
		}
	}
	cityQueryDur := time.Since(start)
	fmt.Printf("100 City queries: %v (%.2fms/query)\n", cityQueryDur, float64(cityQueryDur.Milliseconds())/100)

	start = time.Now()
	scanCount := 0
	users.Scan(func(u PerfUser) bool {
		scanCount++
		return true
	})
	scanDur := time.Since(start)
	fmt.Printf("Full scan of %d records: %v\n", scanCount, scanDur)

	if scanCount != targetRecords {
		t.Errorf("Scan count mismatch: expected %d, got %d", targetRecords, scanCount)
	}
}

func TestMultipleTables(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-table test in short mode")
	}

	dbPath := "/tmp/test_multi_table.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	companyDB, err := New[Company](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create company database: %v", err)
	}

	orderDB, err := New[Order](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create order database: %v", err)
	}

	companies, err := companyDB.Table("companies")
	if err != nil {
		t.Fatalf("Failed to get companies table: %v", err)
	}

	orders, err := orderDB.Table("orders")
	if err != nil {
		t.Fatalf("Failed to get orders table: %v", err)
	}

	companies.CreateIndex("City")
	companies.CreateIndex("Employees")
	orders.CreateIndex("CustomerID")
	orders.CreateIndex("Status")
	orders.CreateIndex("Price")

	const numCompanies = 5000
	const numOrders = 5000

	fmt.Printf("Inserting %d companies...\n", numCompanies)
	companyStart := time.Now()
	for i := 0; i < numCompanies; i++ {
		_, err := companies.Insert(&Company{
			Name:      fmt.Sprintf("Company_%d", i),
			City:      fmt.Sprintf("City_%d", i%50),
			Employees: 10 + (i % 990),
		})
		if err != nil {
			t.Fatalf("Company insert failed: %v", err)
		}
	}
	fmt.Printf("Companies inserted in %v\n", time.Since(companyStart))

	fmt.Printf("Inserting %d orders...\n", numOrders)
	orderStart := time.Now()
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < numOrders; i++ {
		_, err := orders.Insert(&Order{
			CustomerID: uint32(i % numCompanies),
			Product:    fmt.Sprintf("Product_%d", i%1000),
			Quantity:   1 + (i % 10),
			Price:      float64(10 + (i % 1000)),
			CreatedAt:  baseTime.Add(time.Duration(i%365) * 24 * time.Hour),
			Status:     []string{"pending", "processing", "shipped", "delivered"}[i%4],
		})
		if err != nil {
			t.Fatalf("Order insert failed: %v", err)
		}
	}
	fmt.Printf("Orders inserted in %v\n", time.Since(orderStart))

	companyCount := companies.Count()
	orderCount := orders.Count()
	fmt.Printf("\nTotal: %d companies, %d orders\n", companyCount, orderCount)

	if companyCount != numCompanies {
		t.Errorf("Expected %d companies, got %d", numCompanies, companyCount)
	}
	if orderCount != numOrders {
		t.Errorf("Expected %d orders, got %d", numOrders, orderCount)
	}

	start := time.Now()
	for i := 0; i < 50; i++ {
		results, err := companies.Query("City", "City_25")
		if err != nil {
			t.Fatalf("Company query failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("Expected company results")
		}
	}
	fmt.Printf("50 company City queries: %v\n", time.Since(start))

	start = time.Now()
	for i := 0; i < 50; i++ {
		results, err := orders.Query("Status", "pending")
		if err != nil {
			t.Fatalf("Order query failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("Expected order results")
		}
	}
	fmt.Printf("50 order Status queries: %v\n", time.Since(start))

	rangeResults, err := orders.QueryRangeBetween("Price", 100.0, 500.0, true, true)
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
	fmt.Printf("Orders with Price 100-500: %d records\n", len(rangeResults))
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	dbPath := "/tmp/test_concurrent_rw.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	users.CreateIndex("Age")
	users.CreateIndex("City")
	users.CreateIndex("Status")

	const (
		numWriters  = 4
		numReaders  = 8
		recordsEach = 25000
	)

	var totalWritten atomic.Int64
	var totalRead atomic.Int64
	var writeErrors atomic.Int64
	var readErrors atomic.Int64

	start := time.Now()

	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			base := writerID * recordsEach
			for i := 0; i < recordsEach; i++ {
				_, err := users.Insert(&PerfUser{
					Name:     fmt.Sprintf("Writer%d_User_%d", writerID, i),
					Age:      20 + ((base + i) % 80),
					City:     fmt.Sprintf("City_%d", (base+i)%50),
					Category: fmt.Sprintf("Cat_%d", (base+i)%20),
					Status:   []string{"active", "inactive", "pending"}[(base+i)%3],
				})
				if err != nil {
					writeErrors.Add(1)
				} else {
					totalWritten.Add(1)
				}
			}
		}(w)
	}

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			queries := 0
			for {
				if queries >= 100 {
					break
				}
				age := 20 + (readerID*10+queries)%80
				_, err := users.Query("Age", age)
				if err != nil {
					readErrors.Add(1)
				} else {
					totalRead.Add(1)
				}
				queries++
			}
		}(r)
	}

	wg.Wait()
	totalDur := time.Since(start)

	totalRecords := numWriters * recordsEach
	fmt.Printf("\n=== Concurrent Read/Write Test ===\n")
	fmt.Printf("Writers: %d, Readers: %d\n", numWriters, numReaders)
	fmt.Printf("Records written: %d / %d\n", totalWritten.Load(), totalRecords)
	fmt.Printf("Write errors: %d\n", writeErrors.Load())
	fmt.Printf("Read operations: %d\n", totalRead.Load())
	fmt.Printf("Read errors: %d\n", readErrors.Load())
	fmt.Printf("Total time: %.2fs\n", totalDur.Seconds())
	fmt.Printf("Write rate: %.0f rec/sec\n", float64(totalWritten.Load())/totalDur.Seconds())

	count := users.Count()
	fmt.Printf("Final count: %d\n", count)

	if writeErrors.Load() > 0 {
		t.Errorf("Had %d write errors", writeErrors.Load())
	}
	if readErrors.Load() > 0 {
		t.Errorf("Had %d read errors", readErrors.Load())
	}
}

func TestNestedStructQueries(t *testing.T) {
	dbPath := "/tmp/test_nested_queries.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[EmployeeWithNested](dbPath, false, true)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	employees, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	employees.CreateIndex("Salary")
	employees.CreateIndex("Department.Name")
	employees.CreateIndex("Department.Code")
	employees.CreateIndex("Department.Office.City")
	employees.CreateIndex("Manager.Name")

	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		emp := &EmployeeWithNested{
			Name: fmt.Sprintf("Employee_%d", i),
			Department: Department{
				Name: fmt.Sprintf("Dept_%c", 'A'+byte(i%5)),
				Code: fmt.Sprintf("D%02d", i%10),
				Office: Office{
					City:     fmt.Sprintf("City_%d", i%5),
					Floor:    (i % 5) + 1,
					Building: fmt.Sprintf("Building_%c", 'A'+byte(i%3)),
				},
			},
			Manager: Manager{
				Name:  fmt.Sprintf("Manager_%d", i%10),
				Title: fmt.Sprintf("Title_%d", i%5),
			},
			Salary:   30000 + (i * 1000),
			HireDate: baseTime.AddDate(0, i, 0),
		}
		_, err := employees.Insert(emp)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	results, err := employees.Query("Department.Name", "Dept_A")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Employees in Dept_A: %d\n", len(results))
	if len(results) != 20 {
		t.Errorf("Expected 20 employees in Dept_A, got %d", len(results))
	}

	results, err = employees.Query("Department.Office.City", "City_2")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Employees in City_2: %d\n", len(results))
	if len(results) != 20 {
		t.Errorf("Expected 20 employees in City_2, got %d", len(results))
	}

	results, err = employees.Query("Manager.Name", "Manager_5")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("Employees with Manager_5: %d\n", len(results))
	if len(results) != 10 {
		t.Errorf("Expected 10 employees with Manager_5, got %d", len(results))
	}

	rangeResults, err := employees.QueryRangeBetween("Salary", 50000, 70000, true, true)
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
	fmt.Printf("Employees with Salary 50k-70k: %d\n", len(rangeResults))
	if len(rangeResults) < 20 || len(rangeResults) > 22 {
		t.Errorf("Expected 20-22 employees with Salary 50k-70k, got %d", len(rangeResults))
	}
}

func TestPagedQueries(t *testing.T) {
	dbPath := "/tmp/test_paged_queries.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	users.CreateIndex("Age")
	users.CreateIndex("Status")

	for i := 0; i < 1000; i++ {
		users.Insert(&PerfUser{
			Name:     fmt.Sprintf("User_%d", i),
			Age:      20 + (i % 50),
			City:     fmt.Sprintf("City_%d", i%20),
			Category: fmt.Sprintf("Cat_%d", i%10),
			Status:   []string{"active", "inactive", "pending"}[i%3],
		})
	}

	t.Run("QueryRangeBetweenPaged", func(t *testing.T) {
		const pageSize = 50
		offset := 0
		totalCollected := 0
		var lastResult *PagedResult[PerfUser]

		for {
			result, err := users.QueryRangeBetweenPaged("Age", 30, 40, true, true, offset, pageSize)
			if err != nil {
				t.Fatalf("Paged query failed: %v", err)
			}
			lastResult = result

			totalCollected += len(result.Records)
			fmt.Printf("Page offset %d: got %d records (total: %d, hasMore: %v)\n",
				offset, len(result.Records), result.TotalCount, result.HasMore)

			for _, rec := range result.Records {
				if rec.Age < 30 || rec.Age > 40 {
					t.Errorf("Age %d out of range [30,40]", rec.Age)
				}
			}

			if !result.HasMore {
				break
			}
			offset += pageSize
		}

		fmt.Printf("Total collected: %d / %d\n", totalCollected, 1000/5)
		if lastResult != nil && totalCollected != int(lastResult.TotalCount) {
			t.Errorf("Total mismatch: collected %d, reported %d", totalCollected, lastResult.TotalCount)
		}
	})

	t.Run("QueryRangeGreaterThanPaged", func(t *testing.T) {
		result, err := users.QueryRangeGreaterThanPaged("Age", 60, false, 0, 100)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		fmt.Printf("Age > 60: %d total, first page: %d records\n", result.TotalCount, len(result.Records))

		for _, rec := range result.Records {
			if rec.Age <= 60 {
				t.Errorf("Age %d should be > 60", rec.Age)
			}
		}
	})

	t.Run("FilterPaged", func(t *testing.T) {
		offset := 0
		pageSize := 25
		totalCollected := 0

		for {
			result, err := users.FilterPaged(func(u PerfUser) bool {
				return u.Age > 45 && u.Status == "active"
			}, offset, pageSize)
			if err != nil {
				t.Fatalf("FilterPaged failed: %v", err)
			}

			totalCollected += len(result.Records)
			if len(result.Records) > 0 {
				fmt.Printf("Page %d: %d records (total: %d)\n", offset/pageSize, len(result.Records), result.TotalCount)
			}

			if !result.HasMore {
				break
			}
			offset += pageSize
		}

		fmt.Printf("Filter: Age > 45 && Status = active: %d total\n", totalCollected)
	})
}

func TestPartialTextMatching(t *testing.T) {
	dbPath := "/tmp/test_partial_text.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[Order](dbPath, false, true)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	orders, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	products := []string{
		"Apple iPhone 15 Pro",
		"Apple MacBook Air M3",
		"Samsung Galaxy S24 Ultra",
		"Samsung 65-inch OLED TV",
		"Sony WH-1000XM5 Headphones",
		"LG Refrigerator",
		"Microsoft Surface Pro 9",
		"Dell XPS 15 Laptop",
		"HP LaserJet Printer",
		"Canon EOS R5 Camera",
	}

	for i := 0; i < 1000; i++ {
		orders.Insert(&Order{
			CustomerID: uint32(i % 100),
			Product:    products[i%len(products)],
			Quantity:   1 + (i % 5),
			Price:      float64(100 + (i%100)*10),
			CreatedAt:  baseTime.Add(time.Duration(i) * time.Hour),
			Status:     []string{"pending", "processing", "shipped", "delivered"}[i%4],
		})
	}

	t.Run("Contains substring", func(t *testing.T) {
		results, err := orders.Filter(func(o Order) bool {
			return strings.Contains(o.Product, "iPhone")
		})
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		fmt.Printf("Products containing 'iPhone': %d\n", len(results))
		if len(results) != 100 {
			t.Errorf("Expected 100 iPhone orders, got %d", len(results))
		}
	})

	t.Run("Starts with prefix", func(t *testing.T) {
		results, err := orders.Filter(func(o Order) bool {
			return strings.HasPrefix(o.Product, "Apple")
		})
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		fmt.Printf("Products starting with 'Apple': %d\n", len(results))
		if len(results) != 200 {
			t.Errorf("Expected 200 Apple products, got %d", len(results))
		}
	})

	t.Run("Contains multiple brands", func(t *testing.T) {
		results, err := orders.Filter(func(o Order) bool {
			return strings.Contains(o.Product, "Samsung") || strings.Contains(o.Product, "Sony")
		})
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		fmt.Printf("Products with Samsung or Sony: %d\n", len(results))
		if len(results) != 300 {
			t.Errorf("Expected 300 Samsung/Sony products, got %d", len(results))
		}
	})

	t.Run("Case insensitive contains", func(t *testing.T) {
		results, err := orders.Filter(func(o Order) bool {
			return strings.Contains(strings.ToLower(o.Product), "laptop")
		})
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		fmt.Printf("Products containing 'laptop' (case-insensitive): %d\n", len(results))
		if len(results) != 100 {
			t.Errorf("Expected 100 laptop products, got %d", len(results))
		}
	})
}

func TestPagedQueriesWithLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large paged test in short mode")
	}

	dbPath := "/tmp/test_large_paged.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	users.CreateIndex("Age")
	users.CreateIndex("Status")

	const numRecords = 50000
	fmt.Printf("Inserting %d records...\n", numRecords)
	start := time.Now()
	for i := 0; i < numRecords; i++ {
		users.Insert(&PerfUser{
			Name:     fmt.Sprintf("User_%d", i),
			Age:      20 + (i % 100),
			City:     fmt.Sprintf("City_%d", i%50),
			Category: fmt.Sprintf("Cat_%d", i%20),
			Status:   []string{"active", "inactive", "pending"}[i%3],
		})
		if (i+1)%12500 == 0 {
			fmt.Printf("Inserted %d records...\n", i+1)
		}
	}
	fmt.Printf("Inserted in %v\n", time.Since(start))

	fmt.Printf("\nTesting paged queries on %d records...\n", numRecords)
	start = time.Now()

	const pageSize = 500
	offset := 0
	pageCount := 0
	totalCollected := 0

	for {
		result, err := users.QueryRangeBetweenPaged("Age", 40, 60, true, true, offset, pageSize)
		if err != nil {
			t.Fatalf("Paged query failed: %v", err)
		}

		pageCount++
		totalCollected += len(result.Records)

		for _, rec := range result.Records {
			if rec.Age < 40 || rec.Age > 60 {
				t.Errorf("Age %d out of range [40,60]", rec.Age)
			}
		}

		if !result.HasMore {
			break
		}
		offset += pageSize

		if pageCount > 1000 {
			t.Fatal("Too many pages - likely infinite loop")
		}
	}

	dur := time.Since(start)
	fmt.Printf("Paged query completed:\n")
	fmt.Printf("  Pages: %d\n", pageCount)
	fmt.Printf("  Total records: %d\n", totalCollected)
	fmt.Printf("  Time: %v\n", dur)
	fmt.Printf("  Rate: %.0f records/sec\n", float64(totalCollected)/dur.Seconds())

	expectedCount := int(float64(numRecords) * 0.21)
	if totalCollected < expectedCount-500 || totalCollected > expectedCount+500 {
		t.Errorf("Expected ~%d records (21%% of %d), got %d", expectedCount, numRecords, totalCollected)
	}
}

func TestConcurrentReadsDuringWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent read/write test in short mode")
	}

	dbPath := "/tmp/test_concurrent_read_write.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[PerfUser](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	users.CreateIndex("Age")

	const (
		numWriters  = 2
		numReaders  = 4
		recordsEach = 50000
		readCycles  = 50
	)

	var wg sync.WaitGroup
	var stopFlag atomic.Bool
	var consistentReadFailures atomic.Int64
	var readCount atomic.Int64
	var writeCount atomic.Int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for w := 0; w < numWriters; w++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()
				base := writerID * recordsEach
				for i := 0; i < recordsEach; i++ {
					if stopFlag.Load() {
						return
					}
					_, err := users.Insert(&PerfUser{
						Name:     fmt.Sprintf("W%d_U%d", writerID, i),
						Age:      20 + (base+i)%80,
						City:     fmt.Sprintf("City_%d", i%10),
						Category: fmt.Sprintf("Cat_%d", i%5),
						Status:   "active",
					})
					if err == nil {
						writeCount.Add(1)
					}
				}
			}(w)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for cycle := 0; cycle < readCycles; cycle++ {
				if stopFlag.Load() {
					return
				}

				age := 30 + (readerID*10+cycle)%50
				_, err := users.Query("Age", age)
				if err != nil {
					consistentReadFailures.Add(1)
				} else {
					readCount.Add(1)
				}

				count := users.Count()
				if count < 0 {
					consistentReadFailures.Add(1)
				}
			}
		}(r)
	}

	time.Sleep(2 * time.Second)
	stopFlag.Store(true)

	wg.Wait()

	fmt.Printf("\n=== Concurrent Read/Write Consistency Test ===\n")
	fmt.Printf("Writes completed: %d\n", writeCount.Load())
	fmt.Printf("Reads completed: %d\n", readCount.Load())
	fmt.Printf("Read failures: %d\n", consistentReadFailures.Load())
	fmt.Printf("Final count: %d\n", users.Count())

	if consistentReadFailures.Load() > 0 {
		t.Errorf("Had %d read failures during concurrent writes", consistentReadFailures.Load())
	}
}

func TestMillionRecordsWithVacuum(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping million+vacuum test in short mode")
	}

	type DeletableUser struct {
		ID     uint32 `db:"id,primary"`
		Name   string
		Age    int    `db:"index"`
		City   string `db:"index"`
		Status string
	}

	dbPath := "/tmp/test_million_vacuum.db"
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	db, err := New[DeletableUser](dbPath, false, false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	users, err := db.Table()
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	users.CreateIndex("Age")
	users.CreateIndex("City")

	const initialRecords = 100000
	fmt.Printf("Inserting %d initial records...\n", initialRecords)
	start := time.Now()
	for i := 0; i < initialRecords; i++ {
		users.Insert(&DeletableUser{
			Name:   fmt.Sprintf("User_%d", i),
			Age:    20 + (i % 80),
			City:   fmt.Sprintf("City_%d", i%50),
			Status: "active",
		})
		if (i+1)%100000 == 0 {
			fmt.Printf("Inserted %d / %d\n", i+1, initialRecords)
		}
	}
	fmt.Printf("Initial insert: %v\n", time.Since(start))

	stat, _ := os.Stat(dbPath)
	initialSize := stat.Size()
	fmt.Printf("Initial file size: %d bytes\n", initialSize)

	fmt.Printf("\nDeleting 50%% of records...\n")
	deleteStart := time.Now()
	deleteCount := 0
	records, _ := users.Filter(func(u DeletableUser) bool { return true })
	for i, rec := range records {
		if i%2 == 0 {
			users.Delete(rec.ID)
			deleteCount++
		}
	}
	fmt.Printf("Deleted %d records in %v\n", deleteCount, time.Since(deleteStart))

	rawCount := users.Count()
	fmt.Printf("Raw count (includes deleted): %d\n", rawCount)

	activeRecords, _ := users.Filter(func(u DeletableUser) bool { return true })
	actualCount := len(activeRecords)
	fmt.Printf("Actual active count: %d (expected ~%d)\n", actualCount, initialRecords/2)

	if actualCount < initialRecords/2-500 || actualCount > initialRecords/2+500 {
		t.Errorf("Expected ~%d active records, got %d", initialRecords/2, actualCount)
	}

	stat2, _ := os.Stat(dbPath)
	fmt.Printf("File size before vacuum: %d bytes\n", stat2.Size())

	fmt.Printf("\nRunning vacuum...\n")
	vacuumStart := time.Now()
	if err := db.Vacuum(); err != nil {
		t.Fatalf("Vacuum failed: %v", err)
	}
	fmt.Printf("Vacuum completed in %v\n", time.Since(vacuumStart))

	stat3, _ := os.Stat(dbPath)
	fmt.Printf("File size after vacuum: %d bytes (%.1f%% reduction)\n",
		stat3.Size(), float64(initialSize-stat3.Size())/float64(initialSize)*100)

	countAfterVacuum := users.Count()
	activeAfterVacuum, _ := users.Filter(func(u DeletableUser) bool { return true })
	fmt.Printf("Count after vacuum: %d (raw), %d (active)\n", countAfterVacuum, len(activeAfterVacuum))
}
