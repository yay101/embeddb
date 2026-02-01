# EmbedDB

EmbedDB is a lightweight, embedded database written in Go. It provides a type-safe, efficient way to store and retrieve structured data using Go generics, with no code generation required.

## Features

- **Type-safe**: Uses Go generics for type safety without code generation
- **Embedded**: Single file database with no external dependencies
- **Indexing**: B-tree indexes for fast queries on any field
- **Portable**: Database file includes embedded indexes
- **Concurrent**: Thread-safe operations with transaction support
- **Memory-efficient**: Memory-mapped files with minimal memory overhead
- **Go-idiomatic**: Simple API designed to feel natural to Go developers

## Installation

```go
go get -u github.com/yay101/embeddb
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
    ID    uint32 `db:"id,primary"`
    Name  string `db:"index"`
    Email string `db:"unique,index"`
    Age   int
}

func main() {
    // Create a new database
    db, err := embeddb.New[User]("users.db", true, true)
    if err != nil {
        log.Fatalf("Failed to create database: %v", err)
    }
    defer db.Close()

    // Insert a user
    user := &User{
        Name:  "John Doe",
        Email: "john@example.com",
        Age:   30,
    }
    id, err := db.Insert(user)
    if err != nil {
        log.Fatalf("Failed to insert user: %v", err)
    }
    fmt.Printf("Inserted user with ID: %d\n", id)

    // Retrieve a user by ID
    retrievedUser, err := db.Get(id)
    if err != nil {
        log.Fatalf("Failed to retrieve user: %v", err)
    }
    fmt.Printf("Retrieved user: %+v\n", retrievedUser)

    // Query users by Name (using index)
    users, err := db.Query("Name", "John Doe")
    if err != nil {
        log.Fatalf("Failed to query users: %v", err)
    }
    fmt.Printf("Found %d users with name 'John Doe'\n", len(users))
}
```

## Database Operations

### Creating a Database

```go
// Create a new database with no migration and no auto-indexing
db, err := embeddb.New[MyStruct]("mydata.db", false, false)

// Create with migration enabled (will migrate if struct has changed)
db, err := embeddb.New[MyStruct]("mydata.db", true, false)

// Create with auto-indexing enabled (fields with `db:"index"` tag)
db, err := embeddb.New[MyStruct]("mydata.db", false, true)
```

### Basic CRUD Operations

```go
// Insert a record
id, err := db.Insert(&myRecord)

// Get a record by ID
record, err := db.Get(id)

// Update a record
err := db.Update(id, &updatedRecord)

// Delete a record
err := db.Delete(id)
```

### Closing the Database

Always close the database when you're done with it to ensure all data is properly flushed to disk and any indexes are saved:

```go
err := db.Close()
```

## Indexing and Querying

EmbedDB provides B-tree indexes for fast queries on any field of your struct.

### Creating and Using Indexes

There are three ways to create indexes:

1. **Using struct tags** (with auto-indexing enabled):

```go
type Person struct {
    ID    uint32 `db:"id,primary"`
    Name  string `db:"index"`     // Will be indexed
    Email string `db:"unique,index"` // Unique index
    Age   int    // No index
}

// Enable auto-indexing during database creation
db, err := embeddb.New[Person]("people.db", true, true)
```

2. **Explicitly creating indexes**:

```go
// Create an index on any field
err := db.CreateIndex("Age")
```

3. **Dropping indexes**:

```go
// Remove an index when no longer needed
err := db.DropIndex("Age")
```

### Querying with Indexes

Once an index is created, you can perform efficient queries:

```go
// Query by an indexed field
results, err := db.Query("Name", "John Doe")

// Query by another indexed field
youngPeople, err := db.Query("Age", 25)
```

### Index Management

The B-tree indexes are stored in separate files but are embedded in the database file when closed, ensuring portability. When the database is opened, indexes are automatically extracted and used.

## Performance Optimization

### Vacuuming the Database

Over time, as records are updated and deleted, the database file might contain unused space. You can compact the database to reclaim this space:

```go
err := db.Vacuum()
```

This operation is atomic and will maintain all indexes.

## Struct Requirements

Your struct should follow these guidelines:

- Fields should be exported (start with a capital letter)
- Use basic Go types (int, string, bool, etc.) for best compatibility
- You can use nested structs
- Use struct tags for primary keys, unique constraints, and indexes

## Struct Tags

EmbedDB supports several struct tags to control how fields are handled:

- `db:"id,primary"` - Marks a field as the primary key
- `db:"index"` - Creates an index on this field
- `db:"unique,index"` - Creates a unique index on this field

## Technical Details

- Memory-mapped B-tree indexes for efficient querying
- Unsafe pointer field access for maximum performance
- Batched operations for improved write efficiency
- Bloom filters to speed up negative lookups
- Transaction support for atomic operations

## License

This project is licensed under the MIT License - see the LICENSE file for details.