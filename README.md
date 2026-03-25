# Warning

This project is in testing and used in production in a non life threatening situation. It has many hundreds of hours of human development and many hours of AI breaking things but is new to being shared with others. Do not use if you really can't lose your data.

# EmbedDB

A lightweight, embedded database for Go that gives you SQLite-like functionality with a more Go-native experience. Perfect for desktop apps, CLI tools, local caching, and anywhere you'd reach for SQLite but want something simpler.

## Features

- **Zero dependencies** - Just Go standard library + mmapped files
- **Type-safe** - Full Go generics support, no code generation
- **Single file** - Database and indexes in your project directory
- **B-tree indexes** - Fast queries on any indexed field
- **Range queries** - Query by greater than, less than, or between ranges
- **Nested structs** - Query fields like `Address.City` with dot notation
- **Multiple tables** - Store different struct types in one database file
- **Paged queries** - Efficient pagination for large result sets
- **Partial text matching** - Use Filter with `strings.Contains` for text search
- **Concurrent access** - Safe multi-goroutine reads and writes
- **time.Time support** - Full indexing and range queries on timestamps
- **Memory-efficient** - Memory-mapped I/O keeps heap usage minimal
- **Scanner** - Low-lock-contention sequential access for large scans
- **Auto vacuum** - Automatic file compaction for long-running workloads

## Recent Releases

### v1.0.0

**Breaking Changes:**
- Removed `Database.Filter()` and `Database.FilterPaged()` - use `Table.Filter()` and `Table.FilterPaged()` instead

**New Features:**
- `Table.All()` - Returns all records in the table
- `Table.AllPaged(offset, limit)` - Returns paginated records with total count

**Performance:**
- `Table.FilterPaged()` now uses efficient direct reads instead of calling `Get()` per record

### v0.6.1

- Minor bug fixes and documentation updates

### v0.6.0

- Region-backed secondary indexes (only supported storage path)
- Performance optimizations:
  - Per-instance mutexes to reduce lock contention
  - Pre-allocated buffers for record encoding/decoding
  - Field offset cache for O(1) lookup vs O(n) linear search
  - Optimized string hashing using `strconv` instead of `fmt.Sprintf`
- ~25k records/sec insert rate with multiple indexes
- ~1-4ms per indexed query on 200k+ record datasets

## Installation

```bash
go get github.com/yay101/embeddb
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/yay101/embeddb"
)

type User struct {
    ID        uint32    `db:"id,primary"`
    Name      string    `db:"index"`
    Email     string    `db:"unique,index"`
    Age       int       `db:"index"`
    Balance   float64   `db:"index"`
    IsActive  bool      `db:"index"`
    CreatedAt time.Time `db:"index"`
}

func main() {
    db, err := embeddb.Open("users.db", embeddb.OpenOptions{AutoIndex: true})
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    users, err := embeddb.Use[User](db, "users")
    if err != nil {
        log.Fatal(err)
    }

    // Insert
    id, _ := users.Insert(&User{
        Name:      "Alice",
        Email:     "alice@example.com",
        Age:       30,
        Balance:   100.50,
        IsActive:  true,
    })

    // Get by ID
    user, _ := users.Get(id)

    // Query by exact match
    results, _ := users.Query("Name", "Alice")

    // Query by range
    adults, _ := users.QueryRangeGreaterThan("Age", 18, true)

    fmt.Println(user, len(results), len(adults))
}
```

## Supported Field Types

EmbedDB supports all common Go types:

```go
type Record struct {
    // Basic types
    ID        uint32    `db:"id,primary"`  // Primary key
    Name      string    `db:"index"`        // String (indexed)
    Age       int       `db:"index"`        // Integer
    Score     float64   `db:"index"`        // Float
    Amount    int64     `db:"index"`        // Large integers  
    Ratio     float32   `db:"index"`        // Smaller floats
    IsActive  bool      `db:"index"`        // Boolean
    Category  uint32    `db:"index"`        // Unsigned int
    
    // time.Time - fully supported
    CreatedAt time.Time `db:"index"`
    UpdatedAt time.Time
    
    // Nested structs - access with dot notation
    Address   Address
    
    // Embedded time.Time in nested struct
    Metadata  Metadata
    
    // Skip fields (not stored in database)
    TempData []byte `db:"-"`
}

type Address struct {
    City    string `db:"index"`   // Query: "Address.City"
    ZipCode int    `db:"index"`   // Query: "Address.ZipCode"
}

type Metadata struct {
    Version   int       `db:"index"`
    DeletedAt time.Time `db:"index"`  // Query: "Metadata.DeletedAt"
}
```

## Primary Keys

Any field can be marked as the primary key with `db:"id,primary"`. EmbedDB auto-generates an internal `uint32` ID for every record, but you can use any field as your lookup key.

### String Primary Key (e.g., UUID, email)

```go
type User struct {
    UUID   string `db:"id,primary"`  // Used for Get/Update/Delete
    Name   string
    Email  string `db:"index"`
}

db, _ := embeddb.Open("users.db", embeddb.OpenOptions{AutoIndex: true})
users, _ := embeddb.Use[User](db, "users")

// Insert - UUID is stored and indexed automatically
users.Insert(&User{UUID: "user-123", Name: "Alice", Email: "alice@example.com"})

// Get by UUID (string)
user, _ := users.Get("user-123")

// Update by UUID
users.Update("user-123", &User{UUID: "user-123", Name: "Alice Updated"})

// Delete by UUID
users.Delete("user-123")
```

### Integer Primary Key (e.g., SKU, product code)

```go
type Product struct {
    SKU    int     `db:"id,primary"`  // Used for Get/Update/Delete
    Name   string  `db:"index"`
    Price  float64 `db:"index"`
}

products, _ := embeddb.Use[Product](db, "products")

products.Insert(&Product{SKU: 12345, Name: "Widget", Price: 9.99})
product, _ := products.Get(12345)  // Get by SKU (int)
products.Update(12345, &Product{SKU: 12345, Name: "Updated Widget"})
```

### Traditional uint32 Primary Key

The original behavior - uses the auto-generated internal ID:

```go
type Order struct {
    ID         uint32 `db:"id,primary"`  // uint32 PK (stored in header)
    CustomerID uint32 `db:"index"`
    Total      float64 `db:"index"`
    Status     string `db:"index"`
}

orders, _ := embeddb.Use[Order](db, "orders")

id, _ := orders.Insert(&Order{CustomerID: 1, Total: 99.99, Status: "pending"})
order, _ := orders.Get(id)  // Get by internal uint32 ID
```

### How It Works

- **Internal ID**: Every record gets an auto-generated `uint32` ID stored in the record header
- **User-facing PK**: The field marked with `db:"id,primary"` is used for `Get`/`Update`/`Delete`
- **Auto-indexing**: Non-uint32 PK fields are automatically indexed for fast lookups
- **uint32 PKs**: Stored only in the header (no data duplication), use direct offset lookup

### Skipping Fields

Use `db:"-"` to exclude fields from storage. Useful for temporary data or runtime-only fields:

```go
type Config struct {
    ID       uint32 `db:"id,primary"`
    Name     string
    Settings map[string]string `db:"-"`  // Not stored
    Cache   []byte            `db:"-"`  // Not stored
}
```

## Open Options

```go
db, err := embeddb.Open("app.db")

// Enable migration and/or auto-index for tables opened via Use[T]
db, err := embeddb.Open("app.db", embeddb.OpenOptions{Migrate: true})
db, err := embeddb.Open("app.db", embeddb.OpenOptions{AutoIndex: true})
db, err := embeddb.Open("app.db", embeddb.OpenOptions{Migrate: true, AutoIndex: true})
```

## Tables

EmbedDB supports multiple tables within a single database file. Each table can have its own schema.

```go
db, _ := embeddb.Open("app.db")
defer db.Close()

// Table name defaults to the type name
users, _ := embeddb.Use[User](db) // table name is "User"

// Or specify a custom table name
customers, _ := embeddb.Use[User](db, "customers")

_, _ = users, customers
```

## Legacy New[T] API (compatibility)

`New[T]` remains available as a legacy API:

```go
db, err := embeddb.New[User]("app.db", false, false)  // No migration, no auto-index
db, err := embeddb.New[User]("app.db", true, false)  // With migration
db, err := embeddb.New[User]("app.db", false, true)  // Auto-index fields with db:"index"
```

## Table Operations

```go
// Insert
id, err := users.Insert(&User{Name: "Alice", Age: 30})

// Get by ID
user, err := users.Get(id)

// Update
err := users.Update(id, &User{Name: "Alice", Age: 31})

// Upsert - insert or update based on whether the ID exists
id, inserted, err := users.Upsert("alice@example.com", &User{Email: "alice@example.com", Name: "Alice"})
// inserted=true if new record, inserted=false if updated

// Delete (soft delete)
err := users.Delete(id)

// Query (requires index)
results, err := users.Query("Age", 30)

// Filter (full table scan) - uses Scanner for efficiency
results, err := users.Filter(func(u User) bool {
    return u.Age > 18
})

// Scan - iterate all records
err := users.Scan(func(u User) bool {
    fmt.Println(u.Name)
    return true
})

// Scanner - for efficient sequential access with low lock contention
scanner := users.ScanRecords()
defer scanner.Close()
for scanner.Next() {
    record, _ := scanner.Record()
    fmt.Println(record.Name)
}

// Create index on table
err := users.CreateIndex("Age")

// Drop index
err := users.DropIndex("Age")

// Drop table - soft delete, cleaned up on Vacuum
err := users.Drop()

// Vacuum - compacts file and cleans dropped-table data
err := db.Vacuum()
```

### Auto Vacuum

EmbedDB also performs a conservative automatic vacuum check during `Sync()` / `Close()`:

- at most once per 24 hours
- and only if either:
  - at least one table was dropped since the last vacuum, or
  - at least 50,000 record mutations (insert/update/delete) happened since the last vacuum

This keeps routine writes fast while still compacting files periodically for long-running workloads.

### Multiple Tables

You can store multiple types in the same database file from one `Open` call:

```go
type User struct {
    Name string
    Age  int `db:"index"`
}

type Order struct {
    Product string
    Price   float64
}

db, _ := embeddb.Open("app.db")
defer db.Close()

users, _ := embeddb.Use[User](db, "users")
orders, _ := embeddb.Use[Order](db, "orders")

users.Insert(&User{Name: "Alice"})
orders.Insert(&Order{Product: "Widget", Price: 9.99})
```

Each table keeps its own index block and record offsets inside the same `.db` file.

## CRUD Operations

The examples below assume `users, _ := embeddb.Use[User](db, "users")`.

```go
// Insert - returns new ID
id, err := users.Insert(&user)

// Get by ID
user, err := users.Get(id)

// Update
err := users.Update(id, &user)

// Delete (soft delete)
err := users.Delete(id)

// Close - flushes all data to disk
err := db.Close()
```

## Complete Examples

### E-commerce Application

```go
type Product struct {
    ID          string    `db:"id,primary"`  // String SKU (e.g., "SKU-12345")
    Name        string    `db:"index"`
    Category    string    `db:"index"`
    Price       float64   `db:"index"`
    Stock       int       `db:"index"`
    CreatedAt   time.Time `db:"index"`
    Description string
}

type Order struct {
    ID         string    `db:"id,primary"`  // Order number (e.g., "ORD-20240115-001")
    CustomerID string    `db:"index"`       // Customer UUID
    ProductID  string    `db:"index"`       // Product SKU
    Quantity   int       `db:"index"`
    TotalPrice float64   `db:"index"`
    Status     string    `db:"index"`
    CreatedAt  time.Time `db:"index"`
}

db, _ := embeddb.Open("shop.db", embeddb.OpenOptions{AutoIndex: true})
products, _ := embeddb.Use[Product](db, "products")
orders, _ := embeddb.Use[Order](db, "orders")

// Insert products with string IDs
products.Insert(&Product{
    ID: "LAPTOP-001", Name: "Laptop", Category: "Electronics", Price: 999.99, Stock: 50,
})
products.Insert(&Product{
    ID: "HEADPHONES-001", Name: "Headphones", Category: "Electronics", Price: 79.99, Stock: 200,
})

// Get by product ID
laptop, _ := products.Get("LAPTOP-001")

// Find all electronics under $500
deals, _ := products.Filter(func(p Product) bool {
    return p.Category == "Electronics" && p.Price < 500
})

// Create order with string ID
orders.Insert(&Order{
    ID: "ORD-20240115-001", CustomerID: "cust-123", ProductID: "LAPTOP-001",
    Quantity: 1, TotalPrice: 999.99, Status: "pending",
})

// Get paginated results
result, _ := products.QueryRangeBetweenPaged("Price", 0, 100, true, true, 0, 20)
for page := 0; page < int(result.TotalCount)/20+1; page++ {
    // Process each page...
}
```

### Multi-Table with Different Types

```go
type User struct {
    ID    string `db:"id,primary"`  // UUID
    Name  string `db:"index"`
    Email string `db:"index"`
}

type Post struct {
    ID        string `db:"id,primary"`  // Slug (URL-friendly)
    AuthorID  string `db:"index"`       // User UUID
    Title     string `db:"index"`
    Published bool   `db:"index"`
}

type Comment struct {
    ID        int    `db:"id,primary"`  // Auto-incrementing int
    PostID    string `db:"index"`       // Post slug
    AuthorID  string `db:"index"`       // User UUID
    Content   string
}

// All in the same database file!
db, _ := embeddb.Open("blog.db")
users, _ := embeddb.Use[User](db, "users")
posts, _ := embeddb.Use[Post](db, "posts")
comments, _ := embeddb.Use[Comment](db, "comments")

// Get user by UUID
user, _ := users.Get("user-abc-123")

// Get post by slug
post, _ := posts.Get("my-first-post")

// Get comment by ID (int)
comment, _ := comments.Get(42)

// Create indexes for common queries
users.CreateIndex("Email")
posts.CreateIndex("AuthorID")
posts.CreateIndex("Published")
comments.CreateIndex("PostID")
```

### Pagination with Infinite Scroll

```go
const pageSize = 50
offset := 0

for {
    result, err := products.QueryRangeBetweenPaged("Price", minPrice, maxPrice, true, true, offset, pageSize)
    if err != nil {
        log.Fatal(err)
    }

    for _, product := range result.Records {
        // Display product to user
        fmt.Printf("%s - $%.2f\n", product.Name, product.Price)
    }

    if !result.HasMore {
        break
    }
    offset += pageSize
}
```

### Text Search with Filter

```go
// Find products containing "wireless" (case-insensitive)
wireless, _ := products.Filter(func(p Product) bool {
    return strings.Contains(strings.ToLower(p.Name), "wireless")
})

// Find products with names starting with "Pro"
proProducts, _ := products.Filter(func(p Product) bool {
    return strings.HasPrefix(p.Name, "Pro")
})

// Complex search: electronics with "blue" in description
blueElectronics, _ := products.Filter(func(p Product) bool {
    return p.Category == "Electronics" && 
           strings.Contains(strings.ToLower(p.Description), "blue")
})
```

### Complex Filtering

```go
// Multiple conditions
premiumActive, _ := users.Filter(func(u User) bool {
    return u.Subscription == "premium" && 
           u.IsActive && 
           u.LastLogin.After(time.Now().AddDate(0, -1, 0))
})

// Nested field access
nyCustomers, _ := customers.Filter(func(c Customer) bool {
    return c.Address.State == "NY" && 
           c.Address.ZipCode[:3] == "100" // NYC area
})

// Date range queries
recentOrders, _ := orders.Filter(func(o Order) bool {
    return o.CreatedAt.After(time.Now().AddDate(0, 0, -30))
})
```

### Concurrent Access from Multiple Goroutines

```go
var wg sync.WaitGroup

// Writer goroutines
for i := 0; i < 4; i++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        for j := 0; j < 1000; j++ {
            products.Insert(&Product{
                Name:     fmt.Sprintf("Product_%d_%d", id, j),
                Category: "general",
                Price:    float64(j),
            })
        }
    }(i)
}

// Reader goroutines
for i := 0; i < 8; i++ {
    wg.Add(1)
    go func(id int) {
        defer wg.Done()
        for j := 0; j < 100; j++ {
            products.Query("Category", "electronics")
            products.Count()
        }
    }(i)
}

wg.Wait() // Safe to access concurrently
```

### Scanner for Large Datasets

```go
// Scanner provides low-lock-contention iteration
scanner := products.ScanRecords()
defer scanner.Close()

count := 0
totalValue := 0.0

for scanner.Next() {
    product, _ := scanner.Record()
    count++
    totalValue += product.Price
}

fmt.Printf("Scanned %d products, total value: $%.2f\n", count, totalValue)

// With early exit
scanner = products.ScanRecords()
for scanner.Next() {
    product, _ := scanner.Record()
    if product.Stock == 0 {
        break // Stop at first out-of-stock item
    }
    process(product)
}
```

### Batch Operations

```go
// Bulk insert with progress tracking
batch := []Product{}
for i := 0; i < 10000; i++ {
    batch = append(batch, Product{
        Name:     fmt.Sprintf("Item_%d", i),
        Category: categories[i%len(categories)],
        Price:    float64(rand.Intn(1000)),
    })

    if len(batch) == 1000 {
        for i := range batch {
            products.Insert(&batch[i])
        }
        batch = batch[:0] // Reset slice, keep capacity
        fmt.Printf("Inserted %d records...\n", i+1)
    }
}

// Process remaining
for i := range batch {
    products.Insert(&batch[i])
}
```

### Database Maintenance

```go
// Check record counts
fmt.Printf("Products: %d\n", products.Count())
fmt.Printf("Orders: %d\n", orders.Count())

// Vacuum to reclaim space after bulk deletes
err := db.Vacuum()
if err != nil {
    log.Printf("Vacuum failed: %v", err)
}

// Reopen and verify data integrity
db.Close()
db, _ = embeddb.Open("shop.db")
products, _ = embeddb.Use[Product](db, "products")
allProducts, _ := products.Filter(func(p Product) bool { return true })
fmt.Printf("Products after reopen: %d\n", len(allProducts))
```

## Querying

EmbedDB supports two types of queries:

1. **Indexed queries** - Fast B-tree lookups on indexed fields
2. **Unindexed queries** - Full table scans using Filter/Scan

Use indexed queries when possible for best performance.
All examples below use a table handle, for example `users := embeddb.Use[User](db, "users")`.

### Exact Match (Indexed)

```go
// Find all users named "Alice"
results, err := users.Query("Name", "Alice")

// Find all active users
results, err := users.Query("IsActive", true)

// Find user created at specific time
results, err := users.Query("CreatedAt", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
```

### Range Queries

```go
// Age >= 18 (inclusive)
adults, err := users.QueryRangeGreaterThan("Age", 18, true)

// Age < 65 (exclusive)
seniors, err := users.QueryRangeLessThan("Age", 65, false)

// Balance between 100 and 500 (inclusive)
rangeResults, err := users.QueryRangeBetween("Balance", 100.0, 500.0, true, true)

// Created in year 2024
startOfYear := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
endOfYear := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
thisYear, err := users.QueryRangeBetween("CreatedAt", startOfYear, endOfYear, true, true)
```

### Pagination

All query methods have Paged variants that return paginated results with metadata:

```go
// QueryPaged - exact match with pagination
result, err := users.QueryPaged("Age", 30, 0, 10) // offset=0, limit=10
fmt.Printf("Page 1: %d of %d total\n", len(result.Records), result.TotalCount)
if result.HasMore {
    // Fetch next page
    result, _ = users.QueryPaged("Age", 30, 10, 10) // offset=10
}

// QueryRangeGreaterThanPaged - range query with pagination
result, err := users.QueryRangeGreaterThanPaged("Age", 18, true, 0, 10)

// QueryRangeLessThanPaged - less than with pagination
result, err := users.QueryRangeLessThanPaged("Age", 65, false, 0, 10)

// QueryRangeBetweenPaged - between range with pagination
result, err := users.QueryRangeBetweenPaged("Balance", 100.0, 500.0, true, true, 0, 10)

// FilterPaged - full table scan with pagination
result, err := users.FilterPaged(func(u User) bool {
    return u.Age > 18
}, 0, 10) // Skip first 0, return max 10
```

The `PagedResult[T]` type provides:
- `Records` - the records for the current page
- `TotalCount` - total matching records
- `HasMore` - whether more pages are available
- `Offset` / `Limit` - the pagination parameters used

### Nested Struct Queries

```go
// Define structs with nested types
type User struct {
    ID        string  `db:"id,primary"`  // Can be any type
    Name      string  `db:"index"`
    Address   Address // Nested struct
    Manager   Manager // Another nested level
}

type Address struct {
    Street string
    City   string `db:"index"`        // Query: "Address.City"
    State  string `db:"index"`        // Query: "Address.State"
    Zip    int    `db:"index"`       // Query: "Address.Zip"
    Country Country                  // Deep nesting
}

type Country struct {
    Name     string `db:"index"`     // Query: "Address.Country.Name"
    Code     string `db:"index"`     // Query: "Address.Country.Code"
}

type Manager struct {
    Name  string `db:"index"`        // Query: "Manager.Name"
    Title string `db:"index"`        // Query: "Manager.Title"
}

// Query nested struct fields using dot notation
results, err := users.Query("Address.City", "New York")

// Range query on nested field
results, err := users.QueryRangeBetween("Address.Zip", 10000, 99999, true, true)

// Deep nesting works too
results, err := users.Query("Address.Country.Code", "US")

// Query nested manager field
results, err := users.Query("Manager.Name", "Jane Manager")

// Combine with other indexed fields
premiumNY, _ := users.Filter(func(u User) bool {
    return u.Address.State == "NY" && 
           u.Subscription == "premium"
})
```

### Time-Based Queries

```go
// Define struct with timestamps
type Event struct {
    ID        string    `db:"id,primary"`  // Event code (e.g., "CONF-2024-001")
    Name      string    `db:"index"`
    StartTime time.Time `db:"index"`
    EndTime   time.Time `db:"index"`
    CreatedAt time.Time `db:"index"`
}

// Query by exact timestamp
results, err := events.Query("CreatedAt", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC))

// Events after a certain date
recent, err := events.QueryRangeGreaterThan("StartTime", time.Now(), false)

// Events in a date range
conference, err := events.QueryRangeBetween("StartTime", 
    time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC),
    time.Date(2024, 9, 30, 23, 59, 59, 0, time.UTC),
    true, true)

// Filter for time-based conditions
pastEvents, _ := events.Filter(func(e Event) bool {
    return e.EndTime.Before(time.Now())
})

upcomingWeek, _ := events.Filter(func(e Event) bool {
    return e.StartTime.After(time.Now()) && 
           e.StartTime.Before(time.Now().AddDate(0, 0, 7))
})
```

### Boolean and Enum Queries

```go
type Task struct {
    ID       string `db:"id,primary"`  // Task ID (e.g., "TASK-123")
    Title    string `db:"index"`
    Priority string `db:"index"` // "low", "medium", "high"
    Status   string `db:"index"` // "todo", "in_progress", "done"
    IsUrgent bool   `db:"index"`
}

// Query by boolean
urgent, _ := tasks.Query("IsUrgent", true)

// Query by string enum value
highPriority, _ := tasks.Query("Priority", "high")
inProgress, _ := tasks.Query("Status", "in_progress")

// Combine conditions
urgentInProgress, _ := tasks.Filter(func(t Task) bool {
    return t.IsUrgent && t.Status != "done"
})
```

### Unindexed Queries (Full Table Scan)

For fields without indexes, use `Filter` or `Scan` to perform full table scans:

```go
// Filter - returns all matching records
results, err := users.Filter(func(u User) bool {
    return u.Age > 18 && u.IsActive
})

// Filter with nested struct access
results, err := users.Filter(func(u User) bool {
    return u.Address.City == "New York"
})

// Filter with time
results, err := users.Filter(func(u User) bool {
    return u.CreatedAt.After(time.Now().AddDate(-1, 0, 0)) // Created in last year
})

// Scan - iterate over all records, stop early if needed
err := users.Scan(func(u User) bool {
    fmt.Println(u.Name)
    return true // return false to stop iteration
})

// Count - total records in database (including soft-deleted)
total := users.Count()
```

**Note:** Filter and Scan skip soft-deleted records automatically.

### Sorting Results

Use Go's built-in `sort` package - no need for database-level sorting:

```go
import "sort"

results, _ := users.Filter(func(u User) bool { return u.Age > 18 })

// Sort by age ascending
sort.Slice(results, func(i, j int) bool {
    return results[i].Age < results[j].Age
})

// Sort by multiple fields
sort.Slice(results, func(i, j int) bool {
    if results[i].Age != results[j].Age {
        return results[i].Age < results[j].Age
    }
    return results[i].Name < results[j].Name
})
```

## Index Management

```go
// Create index on any field (even after database creation)
err := users.CreateIndex("Age")
err := users.CreateIndex("Address.City")

// Drop index
err := users.DropIndex("Age")

// Indexes are automatically loaded when you reopen the database
```

## Schema Migration

If you change your struct and already have data, enable migration:

```go
// migrate=true will automatically migrate your data to the new schema
db, err := embeddb.Open("data.db", embeddb.OpenOptions{Migrate: true})
records, err := embeddb.Use[MyStruct](db, "MyStruct")
_ = records

// Legacy New[T] equivalent
db2, err := embeddb.New[MyStruct]("data.db", true, false)
_ = db2
```

## Performance

### Insert Performance
- ~200k-350k records/sec without secondary indexes
- ~25k-30k records/sec with multiple secondary indexes on 200k+ record datasets

### Query Performance  
- Exact match (indexed): ~1-4ms/query for indexed fields on 200k records
- Range queries: ~140ms for large result sets on 200k records
- Filter/Scan (full table): ~300ms for 200k records

### Memory Usage
Memory usage stays minimal even with large datasets:

| Records | File Size | Heap Alloc | OS Memory |
|---------|-----------|------------|-----------|
| 1,000   | 94 KB     | 87 KB      | 8 MB      |
| 10,000  | 954 KB    | 217 KB     | 12 MB     |
| 50,000  | 4.7 MB    | 668 KB     | 12 MB     |
| 200,000 | ~20 MB    | ~1.5 MB    | ~30 MB    |

The database uses memory-mapped I/O, so most of the "Sys" memory is just the mapped file - not heap allocations.

## Why EmbedDB?

| SQLite | EmbedDB |
|--------|---------|
| Cgo required | Pure Go (single module) |
| SQL queries | Go structs & functions |
| Schema migrations | Just change your struct |
| Multiple tables | Multiple table handles |
| ACID transactions | Atomic single-record ops |
| 2MB+ binary | ~50KB Go module |
| Complex setup | Zero-config, embed in your app |

## When to use EmbedDB

- Desktop applications needing local storage
- CLI tools with config/state files
- Local caching layer
- Embedded devices
- Anywhere you'd use SQLite but want simpler code

## When NOT to use EmbedDB

- Complex relational queries with joins (use SQLite)
- High-concurrency write workloads (use PostgreSQL/MySQL)
- Distributed systems (use proper databases)
