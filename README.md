# EmbedDB

A lightweight, embedded database for Go with a clean, type-safe API. Perfect for desktop apps, CLI tools, local caching, and anywhere you'd reach for SQLite but want something simpler.

## Features

- **Pure Go** - No Cgo, no dependencies outside stdlib
- **Type-safe** - Full Go generics support
- **Single file** - Database and indexes in one file
- **Map-based indexes** - Fast O(1) lookups on indexed fields
- **Range queries** - Greater than, less than, between
- **Nested structs** - Query fields like `Address.City`
- **Multiple tables** - Different struct types in one file
- **Efficient pagination** - Built-in paged queries
- **Case-insensitive LIKE** - SQL LIKE patterns
- **Scanner** - Low-lock sequential access with early exit
- **Transactions** - Commit and rollback support
- **Vacuum** - File compaction

## Performance

```
=== Benchmarks (300k total records) ===

--- Insert ---
users.Insert 100k:    290ms  (344,000/sec)
orders.Insert 100k:   251ms  (397,000/sec)
products.Insert 100k: 303ms  (329,000/sec)

Memory: 37 MB
Sync:   444ms
Disk:   14.73 MB (51 bytes/record)

--- Query (indexed) ---
Query by Name:    4.9ms  (2,030,000/sec)
Get by PK:       23ms     (430,000/sec)
Query by Age:    0.3ms   (3,280,000/sec)

--- Scan ---
Scan 100k:       252ms  (396,000 records/sec)
```

## Installation

```bash
go get github.com/yay101/embeddb
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/yay101/embeddb"
)

type User struct {
    ID    uint32 `db:"id,primary"`
    Name  string `db:"index"`
    Email string `db:"unique,index"`
    Age   int    `db:"index"`
}

func main() {
    db, _ := embeddb.Open("/tmp/app.db")
    defer db.Close()

    users, _ := embeddb.Use[User](db, "users")

    // Insert
    id, _ := users.Insert(&User{Name: "Alice", Email: "alice@example.com", Age: 30})

    // Get by ID
    user, _ := users.Get(id)

    // Query by index
    results, _ := users.Query("Name", "Alice")

    // Filter (full scan)
    adults, _ := users.Filter(func(u User) bool {
        return u.Age >= 18
    })

    fmt.Println(user.Name, len(results), len(adults))
}
```

## Struct Tags

| Tag | Description |
|-----|-------------|
| `db:"id"` | Primary key field (for Get/Update/Delete) |
| `db:"primary"` | Same as `db:"id"` - marks primary key |
| `db:"index"` | Create index on this field |
| `db:"unique"` | Enforce uniqueness on insert |
| `db:"-"` | Skip field (not stored) |

**Note:** `db:"id,primary"` is equivalent to `db:"id"` - the comma is just a separator.

## API Reference

### Open & Use

```go
// Open database (creates if not exists)
db, err := embeddb.Open("/tmp/app.db")

// Open with options
db, err := embeddb.Open("/tmp/app.db", embeddb.OpenOptions{
    AutoIndex: true,  // Auto-create indexes for db:"index" fields
    Migrate:   false, // Migrate schema on open
})

// Get table handle
users, err := embeddb.Use[User](db, "users")
```

### CRUD Operations

```go
// Insert - returns new ID
id, err := users.Insert(&User{Name: "Alice", Age: 30})

// Get by ID
user, err := users.Get(id)

// Update
err := users.Update(id, &User{Name: "Alice Updated", Age: 31})

// Delete (soft delete)
err := users.Delete(id)

// Upsert
id, inserted, err := users.Upsert(id, &User{Name: "Alice"})

// Count
count := users.Count()
```

### Query (Indexed Fields)

```go
// Exact match
results, err := users.Query("Name", "Alice")

// Greater than or equal
results, err := users.QueryGreaterOrEqual("Age", 18)

// Less than or equal
results, err := users.QueryLessOrEqual("Age", 65)

// Between (inclusive)
results, err := users.QueryRangeBetween("Age", 18, 65, true, true)

// Not equal
results, err := users.QueryNotEqual("Status", "inactive")

// LIKE patterns (case-insensitive)
// "Smith%" - starts with Smith
// "%Smith" - ends with Smith
// "%Smith%" - contains Smith
results, err := users.QueryLike("Name", "%Smith%")
```

### Filter (Full Table Scan)

```go
// Filter returns all matching records
results, err := users.Filter(func(u User) bool {
    return u.Age >= 18 && u.IsActive
})

// FilterPaged for pagination
result, err := users.FilterPaged(func(u User) bool {
    return u.Age >= 18
}, 0, 10)  // offset, limit
```

### Scanner (Sequential Access)

```go
scanner := users.ScanRecords()
defer scanner.Close()

for scanner.Next() {
    user, _ := scanner.Record()
    fmt.Println(user.Name)
    
    // Early exit supported
    if user.Age > 100 {
        break
    }
}
```

### Pagination

```go
// QueryPaged
result, err := users.QueryPaged("Age", 30, 0, 10)

fmt.Printf("Page 1: %d records (total: %d)\n", 
    len(result.Records), result.TotalCount)
fmt.Printf("Has more: %v\n", result.HasMore)

// Get next page
if result.HasMore {
    next, _ := users.QueryPaged("Age", 30, 10, 10)
}
```

### Transactions

```go
// Begin transaction
db.Begin()

users.Insert(&User{Name: "Alice", Age: 30})
users.Insert(&User{Name: "Bob", Age: 25})

// Commit or Rollback
err := db.Commit()
// err := db.Rollback()
```

### Table & Index Management

```go
// Create secondary index
users.CreateIndex("Email")

// Drop index
users.DropIndex("Email")

// Drop table (soft delete)
users.Drop()

// Vacuum - compact file and cleanup
db.Vacuum()

// Sync - flush to disk
db.Sync()

// Close
db.Close()
```

### Multiple Tables

```go
type Order struct {
    ID         uint32 `db:"id,primary"`
    CustomerID uint32 `db:"index"`
    Total      float64
    Status     string `db:"index"`
}

db, _ := embeddb.Open("/tmp/app.db")

users, _ := embeddb.Use[User](db, "users")
orders, _ := embeddb.Use[Order](db, "orders")

users.Insert(&User{Name: "Alice"})
orders.Insert(&Order{CustomerID: 1, Total: 99.99, Status: "pending"})
```

### Nested Structs

```go
type Address struct {
    Street string
    City   string `db:"index"`
    Zip    string
}

type User struct {
    ID      uint32 `db:"id,primary"`
    Name    string
    Address Address
}

// Query nested fields with dot notation
results, _ := users.Query("Address.City", "New York")

// Filter also works
results, _ := users.Filter(func(u User) bool {
    return u.Address.City == "New York" && u.Address.Zip[:3] == "100"
})
```

## Supported Types

- `string` - indexed, encoded efficiently
- `int`, `int8`, `int16`, `int32`, `int64`
- `uint`, `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `bool`
- `time.Time` - full timestamp support

## File Format

- Header: 64 bytes (magic, version, catalog offset)
- Records: stored sequentially after header (4096+)
- Catalog: table definitions at end of file
- Indexes: in-memory maps, rebuilt on load
- 51 bytes per record overhead + field data

## Why EmbedDB?

| SQLite | EmbedDB |
|--------|---------|
| Cgo required | Pure Go |
| SQL | Go structs |
| Schema migrations | Change your struct |
| Complex setup | Zero-config |

## When to Use

- Desktop apps with local storage
- CLI tools
- Local caching
- Embedded devices
- Anywhere you'd use SQLite but want simpler code

## When NOT to Use

- Complex relational queries (use SQLite)
- High-concurrency writes (use PostgreSQL)
- Distributed systems (use a real database)
