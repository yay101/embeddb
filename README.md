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
    db, err := embeddb.New[User]("users.db", false, true)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Insert
    id, _ := db.Insert(&User{
        Name:      "Alice",
        Email:     "alice@example.com",
        Age:       30,
        Balance:   100.50,
        IsActive:  true,
    })

    // Get by ID
    user, _ := db.Get(id)

    // Query by exact match
    results, _ := db.Query("Name", "Alice")

    // Query by range
    adults, _ := db.QueryRangeGreaterThan("Age", 18, true)
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

## Database Options

```go
// New creates a database
// Parameters: filename, migrate (auto-migrate on struct change), autoIndex (auto-create indexes)

db, err := embeddb.New[User]("app.db", false, false)  // No migration, no auto-index
db, err := embeddb.New[User]("app.db", true, false)  // With migration
db, err := embeddb.New[User]("app.db", false, true)  // Auto-index fields with db:"index"
```

## Tables

EmbedDB supports multiple tables within a single database file. Each table is typed and can have its own schema.

```go
// Get a typed table - table name is auto-derived from the type
db, _ := embeddb.New[User]("app.db", false, false)
users, _ := db.Table()  // table name is "User"

// Or specify a custom table name
users, _ := db.Table("customers")
```

### Table Operations

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

// Filter (full table scan)
results, err := users.Filter(func(u User) bool {
    return u.Age > 18
})

// Scan
err := users.Scan(func(u User) bool {
    fmt.Println(u.Name)
    return true
})

// Create index on table
err := users.CreateIndex("Age")

// Drop index
err := users.DropIndex("Age")
```

### Multiple Tables

You can work with multiple types in the same database by creating separate tables for each type:

```go
type User struct {
    Name string
    Age  int `db:"index"`
}

type Order struct {
    Product string
    Price   float64
}

// Same database, different tables
db, _ := embeddb.New[User]("app.db", false, false)

users, _ := db.Table()
orders, _ := db.Table()

// Each table has its own records
users.Insert(&User{Name: "Alice"})
orders.Insert(&Order{Product: "Widget", Price: 9.99})
```

## CRUD Operations

```go
// Insert - returns new ID
id, err := db.Insert(&user)

// Get by ID
user, err := db.Get(id)

// Update
err := db.Update(id, &user)

// Delete (soft delete)
err := db.Delete(id)

// Close - flushes all data to disk
err := db.Close()
```

## Querying

EmbedDB supports two types of queries:

1. **Indexed queries** - Fast B-tree lookups on indexed fields
2. **Unindexed queries** - Full table scans using Filter/Scan

Use indexed queries when possible for best performance.

### Exact Match (Indexed)

```go
// Find all users named "Alice"
results, err := db.Query("Name", "Alice")

// Find all active users
results, err := db.Query("IsActive", true)

// Find user created at specific time
results, err := db.Query("CreatedAt", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
```

### Range Queries

```go
// Age >= 18 (inclusive)
adults, err := db.QueryRangeGreaterThan("Age", 18, true)

// Age < 65 (exclusive)
seniors, err := db.QueryRangeLessThan("Age", 65, false)

// Balance between 100 and 500 (inclusive)
rangeResults, err := db.QueryRangeBetween("Balance", 100.0, 500.0, true, true)

// Created in year 2024
startOfYear := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
endOfYear := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
thisYear, err := db.QueryRangeBetween("CreatedAt", startOfYear, endOfYear, true, true)
```

### Nested Struct Queries

```go
// Query nested struct fields using dot notation
results, err := db.Query("Address.City", "New York")

// Range query on nested field
results, err := db.QueryRangeBetween("Address.ZipCode", 10000, 99999, true, true)

// Query embedded time in nested struct
results, err := db.Query("Metadata.DeletedAt", time.Time{})
```

### Unindexed Queries (Full Table Scan)

For fields without indexes, use `Filter` or `Scan` to perform full table scans:

```go
// Filter - returns all matching records
results, err := db.Filter(func(u User) bool {
    return u.Age > 18 && u.IsActive
})

// Filter with nested struct access
results, err := db.Filter(func(u User) bool {
    return u.Address.City == "New York"
})

// Filter with time
results, err := db.Filter(func(u User) bool {
    return u.CreatedAt.After(time.Now().AddDate(-1, 0, 0)) // Created in last year
})

// Scan - iterate over all records, stop early if needed
err := db.Scan(func(u User) bool {
    fmt.Println(u.Name)
    return true // return false to stop iteration
})

// Count - total records in database (including soft-deleted)
total := db.Count()
```

**Note:** Filter and Scan skip soft-deleted records automatically.

### Sorting Results

Use Go's built-in `sort` package - no need for database-level sorting:

```go
import "sort"

results, _ := db.Filter(func(u User) bool { return u.Age > 18 })

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
err := db.CreateIndex("Age")
err := db.CreateIndex("Address.City")

// Drop index
err := db.DropIndex("Age")

// Indexes persist with the database file
// They load automatically when you reopen the database
```

## Schema Migration

If you change your struct and already have data, enable migration:

```go
// migrate=true will automatically migrate your data to the new schema
db, err := embeddb.New[MyStruct]("data.db", true, false)
```

## Performance

Memory usage stays minimal even with large datasets:

| Records | File Size | Heap Alloc | OS Memory |
|---------|-----------|------------|-----------|
| 1,000   | 94 KB     | 87 KB      | 12 MB     |
| 10,000  | 954 KB    | 217 KB     | 12 MB     |
| 50,000  | 4.7 MB    | 668 KB     | 12 MB     |

The database uses memory-mapped I/O, so most of the "Sys" memory is just the mapped file - not heap allocations.

## Why EmbedDB?

| SQLite | EmbedDB |
|--------|---------|
| Cgo required | Pure Go |
| SQL queries | Go structs |
| Schema migrations | Just change your struct |
| Multiple tables | Multiple typed tables |
| ACID transactions | Atomic single-record ops |
| 2MB+ binary | Single .go file |

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
