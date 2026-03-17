# EmbedDB

A lightweight, embedded database for Go that gives you SQLite-like functionality with a more Go-native experience. Perfect for desktop apps, CLI tools, local caching, and anywhere you'd reach for SQLite but want something simpler.

## Features

- **Zero dependencies** - Just Go standard library + mmapped files
- **Type-safe** - Full Go generics support, no code generation
- **Single file** - Database and indexes in your project directory
- **B-tree indexes** - Fast queries on any indexed field
- **Range queries** - Query by greater than, less than, or between ranges
- **Nested structs** - Query fields like `Address.City` with dot notation
- **time.Time support** - Full indexing and range queries on timestamps
- **Memory-efficient** - Memory-mapped I/O keeps heap usage minimal
- **Scanner** - Low-lock-contention sequential access for large scans

## Recent Releases

### v0.4.0

- Secondary indexes now use embedded region-backed storage in the main DB file by default.
- Legacy secondary index file paths (`*.idx`) were removed from runtime behavior.
- Existing query features are preserved, including nested struct indexing and range queries.

### v0.3.4

- Added switchable region-backed secondary index storage path.
- Added region pointer/capacity metadata handling and growth safety checks.
- Added regression coverage for reopen persistence and region growth behavior.

## Migration Notes

- Upgrading to `v0.4.0` removes runtime `.idx` files. Secondary indexes persist in the DB file.
- `v0.4.0` expects region-backed secondary indexes to be enabled in normal operation.

- If an old deployment still has stale `.idx` files from pre-`v0.4.0`, they can be safely deleted after successful reopen and query verification.

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
// Query nested struct fields using dot notation
results, err := users.Query("Address.City", "New York")

// Range query on nested field
results, err := users.QueryRangeBetween("Address.ZipCode", 10000, 99999, true, true)

// Query embedded time in nested struct
results, err := users.Query("Metadata.DeletedAt", time.Time{})
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

// Indexes persist with the database file
// They load automatically when you reopen the database
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
- ~350k-400k records/sec without secondary indexes
- ~30k-40k records/sec with multiple secondary indexes on 100k-scale workloads

### Query Performance  
- Exact match (indexed): ~1-2ms/query for moderate-cardinality keys on 100k rows
- Exact match (indexed): ~20-30ms/query for low-cardinality keys on 100k rows
- Filter/Scan (full table): ~80ms for 100k records

### Memory Usage
Memory usage stays minimal even with large datasets:

| Records | File Size | Heap Alloc | OS Memory |
|---------|-----------|------------|-----------|
| 1,000   | 94 KB     | 87 KB      | 8 MB      |
| 10,000  | 954 KB    | 217 KB     | 12 MB     |
| 50,000  | 4.7 MB    | 668 KB     | 12 MB     |

The database uses memory-mapped I/O, so most of the "Sys" memory is just the mapped file - not heap allocations.

## Why EmbedDB?

| SQLite | EmbedDB |
|--------|---------|
| Cgo required | Pure Go |
| SQL queries | Go structs |
| Schema migrations | Just change your struct |
| Multiple tables | Multiple table handles |
| ACID transactions | Atomic single-record ops |
| 2MB+ binary | Pure Go module |

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
