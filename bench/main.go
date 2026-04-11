package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	embeddb "github.com/yay101/embeddb"
)

type User struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Email string `db:"unique,index"`
	Age   int    `db:"index"`
}

type Order struct {
	ID         uint32 `db:"id,primary"`
	CustomerID uint32 `db:"index"`
	Total      float64
	Status     string `db:"index"`
}

type Product struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Price float64
	SKU   string `db:"unique,index"`
}

var memStats runtime.MemStats

func getMemMB() float64 {
	runtime.ReadMemStats(&memStats)
	return float64(memStats.Alloc) / 1024 / 1024
}

func main() {
	os.Remove("/tmp/embeddb_bench.db")
	defer os.Remove("/tmp/embeddb_bench.db")

	fmt.Println("=== EmbedDB Benchmarks (300k total records) ===")
	fmt.Println()

	db, _ := embeddb.Open("/tmp/embeddb_bench.db")
	users, _ := embeddb.Use[User](db, "users")
	orders, _ := embeddb.Use[Order](db, "orders")
	products, _ := embeddb.Use[Product](db, "products")

	fmt.Printf("Open DB: %.2f MB\n", getMemMB())
	fmt.Println()
	fmt.Println("--- Insert ---")

	start := time.Now()
	for i := 0; i < 100000; i++ {
		users.Insert(&User{Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@example.com", i), Age: 20 + i%50})
	}
	fmt.Printf("users.Insert 100k: %v (%d/sec)\n", time.Since(start), int(100000*1000)/int(time.Since(start).Milliseconds()))

	start = time.Now()
	for i := 0; i < 100000; i++ {
		orders.Insert(&Order{CustomerID: uint32(i % 10000), Total: float64(i) * 1.99, Status: "pending"})
	}
	fmt.Printf("orders.Insert 100k: %v (%d/sec)\n", time.Since(start), int(100000*1000)/int(time.Since(start).Milliseconds()))

	start = time.Now()
	for i := 0; i < 100000; i++ {
		products.Insert(&Product{Name: fmt.Sprintf("Product%d", i), Price: float64(i) * 0.99, SKU: fmt.Sprintf("SKU%05d", i)})
	}
	fmt.Printf("products.Insert 100k: %v (%d/sec)\n", time.Since(start), int(100000*1000)/int(time.Since(start).Milliseconds()))

	fmt.Printf("\nMemory after inserts: %.2f MB\n", getMemMB())

	start = time.Now()
	db.Sync()
	fmt.Printf("Sync to disk: %v\n", time.Since(start))

	fileInfo, _ := os.Stat("/tmp/embeddb_bench.db")
	fmt.Printf("Disk usage: %.2f MB (%d bytes/record)\n", float64(fileInfo.Size())/1024/1024, fileInfo.Size()/300000)

	fmt.Println()
	fmt.Println("--- Query (indexed fields) ---")

	start = time.Now()
	for i := 0; i < 1000; i++ {
		users.Query("Name", fmt.Sprintf("User%d", i%10000))
	}
	fmt.Printf("Query Name (index): %v (%d/sec)\n", time.Since(start), int(1000*1000)/int(time.Since(start).Milliseconds()))

	start = time.Now()
	for i := 0; i < 1000; i++ {
		users.Get(uint32(i % 100000))
	}
	fmt.Printf("Get by PK: %v (%d/sec)\n", time.Since(start), int(1000*1000)/int(time.Since(start).Milliseconds()))

	start = time.Now()
	for i := 0; i < 1000; i++ {
		users.Query("Age", 30)
	}
	fmt.Printf("Query Age (index): %v (%d/sec)\n", time.Since(start), int(1000*1000)/int(time.Since(start).Milliseconds()))

	fmt.Println()
	fmt.Println("--- Count & Scan ---")

	start = time.Now()
	users.Count()
	fmt.Printf("Count (100k users): %v\n", time.Since(start))

	start = time.Now()
	scanner := users.ScanRecords()
	count := 0
	for scanner.Next() {
		count++
	}
	scanner.Close()
	elapsed := time.Since(start)
	ms := int(elapsed.Milliseconds())
	if ms == 0 {
		ms = 1
	}
	fmt.Printf("Scan (stream): %v (%d records, %d/sec)\n", elapsed, count, int(count*1000)/ms)

	db.Close()
}
