# EmbedDB

A lightweight, embedded database for Go with a clean, type-safe API. Perfect for desktop apps, CLI tools, local caching, and anywhere you'd reach for SQLite but want something simpler.

## Features

- **Pure Go** - No Cgo, minimal dependencies
- **Type-safe** - Full Go generics support
- **Single file** - Database and indexes in one file
- **B-tree primary index** - Persistent B-tree with Copy-on-Write transactions
- **Range queries** - Greater than, less than, between (indexed and non-indexed)
- **Nested structs** - Query fields like `Address.City`
- **Multiple tables** - Different struct types in one file
- **Efficient pagination** - Built-in paged queries
- **Case-insensitive LIKE** - SQL LIKE patterns
- **Scanner** - Low-lock sequential access with early exit
- **Transactions** - Full Copy-on-Write commit and rollback support
- **Vacuum** - File compaction
- **Auto-indexing** - Automatically creates indexes for `db:"index"` fields
- **Schema migration** - Automatically migrates records when struct changes
- **Index recovery** - Automatically rebuilds indexes on startup if corrupted
- **Auto-sync** - Periodic disk flush (every N writes or idle timeout)
- **FastSync** - Quick fsync without defragmentation
- **Storage modes** - In-memory, memory-mapped (default), or file-only I/O
- **Write-Ahead Log (WAL)** - Crash recovery with append-only logging
- **Compression** - Optional snappy compression for record payloads
- **Backup API** - Consistent database file backup with WAL support
- **Adaptive cache** - B-tree page cache auto-sizes based on hit rate
- **Bulk insert** - Optimized sorted B-tree build for batch inserts
- **Fine-grained locking** - Read operations bypass global lock for better concurrency
- **PK uniqueness** - Error on duplicate primary keys
- **Versioning** - Keep N previous versions of records with timestamps

## Performance

```
=== Benchmarks (300k total records) ===

--- Insert ---
users.Insert 100k:    951ms   (105,000/sec)
orders.Insert 100k:   885ms   (113,000/sec)
products.Insert 100k: 1.03s   (97,000/sec)

Memory: 81.6 MB
Sync:   3.1s
Disk:   67.85 MB (237 bytes/record)

--- Query (indexed) ---
Query by Name:     5.8ms   (172,000/sec)
Get by PK:         2.9ms   (345,000/sec)
Query by Age:      5.5s    (181/sec)

--- Count & Scan ---
Count 100k:        1.1µs   (O(1) via btree counter)
Scan 100k:       293ms    (342,000 records/sec)
```

**Note:** B-tree storage trades insert speed for memory efficiency and persistence. The primary key index is stored as a persistent B-tree, providing crash recovery without index rebuild. Use Vacuum() periodically to defragment.

## Installation

```bash
go get github.com/yay101/embeddb
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"

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
**Note:** Duplicate primary keys return an error on Insert.

## API Reference

### Open & Use

```go
// Open database (creates if not exists)
// AutoIndex and Migrate default to true
db, err := embeddb.Open("/tmp/app.db")

// Open with options (both default to true)
db, err := embeddb.Open("/tmp/app.db", embeddb.OpenOptions{
    AutoIndex: true,  // Auto-create indexes for db:"index" fields
    Migrate:   true,  // Automatically migrate schema changes
})

// Disable auto-indexing or migration
db, err := embeddb.Open("/tmp/app.db", embeddb.OpenOptions{
    AutoIndex:     false,
    Migrate:       false,
    SyncThreshold: 1000,         // fsync every N writes (default 1000)
    IdleThreshold: 10 * time.Second,  // fsync after idle (default 10s)
    StorageMode:   embeddb.StorageMmap, // or StorageFile for no mmap overhead
})

// Get table handle (versioning disabled by default)
users, err := embeddb.Use[User](db, "users")

// Enable versioning - keeps last N versions of each record
docs, err := embeddb.Use[Document](db, embeddb.UseOptions{
    MaxVersions: 5,  // Keep current + 5 previous versions
})
```

### Versioning

Versioning is disabled by default. Enable it per-table using `UseOptions.MaxVersions`:

```go
// Enable versioning when creating a table
docs, err := embeddb.Use[Document](db, embeddb.UseOptions{
    MaxVersions: 5,  // Keep current + 5 previous versions
})

// Insert creates version 1
id, _ := docs.Insert(&Document{Title: "Draft v1", Content: "..."})

// Update creates a new version (v2)
docs.Update(id, &Document{Title: "Draft v2", Content: "..."})

// Get current version (latest)
current, _ := docs.Get(id)

// Get a specific version
v1, _ := docs.GetVersion(id, 1)

// List all versions with metadata
versions, _ := docs.ListVersions(id)
for _, v := range versions {
    fmt.Printf("Version %d created at %v\n", v.Version, v.CreatedAt)
}
```

**Note:** When `MaxVersions > 0`, old versions are marked as deleted during `Vacuum()`. The current version is always accessible via `Get()`.

### Storage Modes

EmbedDB supports two storage backends:

```go
// In-memory - no persistence, fastest for ephemeral data
db, err := embeddb.Open("", embeddb.OpenOptions{
    StorageMode: embeddb.StorageMemory,
})

// Memory-mapped I/O (default) - zero-copy reads, kernel-managed caching
db, err := embeddb.Open("/tmp/app.db", embeddb.OpenOptions{
    StorageMode: embeddb.StorageMmap,
})

// File-only I/O - no mmap overhead, uses ReadAt/WriteAt directly
db, err := embeddb.Open("/tmp/app.db", embeddb.OpenOptions{
    StorageMode: embeddb.StorageFile,
})
```

**StorageMemory**: Pure in-memory storage using anonymous memory mapping. No file I/O, no persistence. Data is lost when the database is closed. WAL is automatically disabled. Best for ephemeral data, testing, or caching.

**StorageMmap** (default): Uses memory-mapped files for zero-copy page access. Best for most workloads.

**StorageFile**: Uses direct file I/O (`ReadAt`/`WriteAt`). Avoids mmap overhead and virtual memory reservation. Useful on constrained systems or when you want explicit I/O control.

### Storage Mode Benchmarks

| Mode | 10k Insert | 50k Insert | 100k Insert | 100k Read | 100k Query(100) |
|------|-----------|-----------|------------|----------|----------------|
| Memory | 63ms | 338ms | 714ms | 23ms | 36µs |
| Mmap | 59ms | 329ms | 820ms | 177ms | 78µs |
| Mmap+WAL | 81ms | 492ms | 962ms | 157ms | 52µs |
| File | 59ms | 442ms | 694ms | 159ms | 32µs |
| File+WAL | 87ms | 457ms | 890ms | 188ms | 31µs |

Memory mode is competitive on inserts and fastest on reads (no disk I/O). Persistent modes show similar performance at scale, with WAL adding ~20-50% overhead on inserts.

### Feature Compatibility

| Feature | StorageMemory | StorageMmap | StorageFile |
|---------|:-------------:|:-----------:|:-----------:|
| WAL | ❌ Error | ✅ | ✅ |
| Compression | ✅ | ✅ | ✅ |
| Encryption | ✅ | ✅ | ✅ |
| Encryption + Compression | ❌ Error | ❌ Error | ❌ Error |
| Backup | ❌ Error | ✅ | ✅ |
| Vacuum | No-op | ✅ | ✅ |

**Incompatible combinations (error at Open/Use time):**
- `WAL + StorageMemory` — WAL requires a file, in-memory has none
- `Encryption + Compression` — encrypted data is incompressible, compression provides zero benefit
- `index + encrypt` on same field — encrypted fields cannot be indexed (error at `Use()` time)

### Write-Ahead Log (WAL)

Enable WAL for crash recovery. All writes are logged to a `.wal` file before being applied to the main database. On startup, uncommitted WAL entries are replayed automatically.

```go
// Enable WAL for crash recovery
db, err := embeddb.Open("/tmp/app.db", embeddb.OpenOptions{
    WAL: true,
})
```

**How it works:**
- Every write is appended to `<dbname>.wal` before being applied to the main file
- On `Sync()`, `Close()`, or `Vacuum()`, the WAL is checkpointed (applied to main file and removed)
- On open, if a WAL file exists, entries are replayed to recover from crashes
- WAL entries include CRC32 checksums for corruption detection

**Trade-offs:**
- Insert throughput is ~1.5-1.7x slower with WAL (double-write to WAL + main file)
- Read performance is similar or slightly better (OS page cache benefits)
- WAL file grows until checkpointed — call `db.Sync()` periodically for long-running write-heavy workloads
- Memory usage and final file size are identical

**Crash recovery:** If the process crashes without calling `Close()`, reopening the database automatically replays the WAL to restore all committed writes.

### Compression

Optional snappy compression for record payloads. Only activates when compressed size is smaller than original.

```go
db, err := embeddb.Open("/tmp/app.db", embeddb.OpenOptions{
    Compression:    true,   // Enable compression
    CompressMinLen: 64,     // Only compress payloads >= 64 bytes (default)
})
```

**Trade-offs:**
- Up to 90% file size reduction on repetitive/compressible data
- Minimal CPU overhead (snappy is designed for speed)
- Transparent — decompression happens automatically on read
- Small payloads (<64 bytes) are not compressed to avoid overhead

### Bulk Insert

Optimized batch insertion that sorts keys and builds the B-tree bottom-up, avoiding per-insert page splits.

```go
records := []*User{ /* ... */ }
ids, err := users.InsertManyBulk(records)
```

**Performance:** ~1.3x faster than `InsertMany` for large batches (50k records: 49k/sec vs 37k/sec).

### Backup API

Creates a consistent copy of the database file, including the WAL if present.

```go
err := db.Backup("/path/to/backup.db")
```

- Atomic: removes partial backup on failure
- Copies both the main database file and WAL file
- Requires database to be open

### Adaptive Cache

The B-tree page cache auto-sizes based on observed hit rate:

- Grows by 1.5x when hit rate > 90% (working set fits, cache is effective)
- Shrinks by 2x when hit rate < 30% (cache is thrashing, wasting memory)
- Clamped between 64 and 16,384 pages
- Adjusts every 5 seconds after at least 100 accesses

```go
// Check cache stats
stats := db.Stats()
cache := stats.CacheStats["primary"]
fmt.Printf("size=%d filled=%d hitRate=%.2f\n",
    cache.Size, cache.Filled, cache.HitRate)
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

// Greater than (indexed)
results, err := users.QueryRangeGreaterThan("Age", 18, false)
results, err := users.QueryRangeGreaterThan("Age", 18, true) // inclusive

// Less than (indexed)
results, err := users.QueryRangeLessThan("Age", 65, false)
results, err := users.QueryRangeLessThan("Age", 65, true) // inclusive

// Between (indexed)
results, err := users.QueryRangeBetween("Age", 18, 65, true, true) // inclusive both
results, err := users.QueryRangeBetween("Age", 18, 65, false, false) // exclusive both

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

Transactions use Copy-on-Write semantics for crash safety:

```go
// Begin transaction
db.Begin()

users.Insert(&User{Name: "Alice", Age: 30})
users.Insert(&User{Name: "Bob", Age: 25})

// Commit or Rollback
err := db.Commit()
// err := db.Rollback()
```

On rollback, both the primary key index (B-tree) and version index are restored to their pre-transaction state.

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

// Sync - flush to disk (full defragmentation)
db.Sync()

// FastSync - quick fsync without defragmentation (used by auto-sync)
db.FastSync()

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

### Slices

All basic Go slice types are supported inside structs:

```go
type Order struct {
    ID        uint32  `db:"id,primary"`
    Items     []string
    Prices    []float64
    Flags     []bool
    Tags      []int
}

db, _ := embeddb.Open("/tmp/app.db")
defer db.Close()

orders, _ := embeddb.Use[Order](db, "orders")

orders.Insert(&Order{
    Items:  []string{"widget", "gadget"},
    Prices: []float64{9.99, 19.99},
    Flags:  []bool{true, false},
    Tags:   []int{1, 2, 3},
})

// Filter on records with slices
results, _ := orders.Filter(func(o Order) bool {
    return len(o.Items) > 0
})

// Slices of structs are also supported
type Item struct {
    Name  string
    Price float64
}

type Cart struct {
    ID    uint32 `db:"id,primary"`
    Owner string
    Items []Item
}

carts, _ := embeddb.Use[Cart](db, "carts")
carts.Insert(&Cart{
    Owner: "Alice",
    Items: []Item{
        {Name: "Widget", Price: 9.99},
        {Name: "Gadget", Price: 19.99},
    },
})
```

## Supported Types

### Scalars (indexed, queryable, sortable)

- `string` - indexed, encoded efficiently
- `int`, `int8`, `int16`, `int32`, `int64`
- `uint`, `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `bool`
- `time.Time` - full timestamp support

### Slices (stored, round-tripped, filterable)

- `[]byte` - binary data
- `[]string` - string arrays
- `[]int`, `[]int8`, `[]int16`, `[]int32`, `[]int64`
- `[]uint`, `[]uint8` (same as `[]byte`), `[]uint16`, `[]uint32`, `[]uint64`
- `[]float32`, `[]float64`
- `[]bool`
- `[]struct` - slices of structs (all scalar and slice fields supported recursively)

### Nested structs

- Named and embedded structs are flattened and their fields become queryable with dot notation (e.g., `Address.City`)

## File Format

- Header: 128 bytes (magic, version, catalog offset, B-tree roots)
- Records: TLV-encoded with CRC verification, optional snappy compression (flagged in record header)
- Catalog: table definitions at end of file
- Primary Index: Persistent B-tree with mmap or file I/O (configurable), Copy-on-Write transactions
- Secondary Indexes: Persistent B-tree indexes with automatic recovery
- Version Index: B-tree tracking record version history
- WAL: Append-only log file (`<dbname>.wal`) for crash recovery, entries have CRC32 checksums
- B-tree nodes: 4096 bytes per node, adaptive LRU cache (auto-sizes 64–16,384 pages based on hit rate)
- Record IDs: uint32 (4 bytes) in secondary key suffix for compact storage

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

## License

[GPL-3.0](LICENSE) — This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
