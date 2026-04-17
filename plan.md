# EmbedDB v2 Rewrite Plan

## Phase 1: Foundation

### 1.1 TLV Record Format
Rework embeddbcore to use length-prefixed fields instead of delimiter-based framing.

**Current problem**: Fields delimited by `0x1E`/`0x1F` markers corrupt when those bytes appear in varint-encoded data.

**New format** (per field):
```
[uvarint name_length][name_bytes][uvarint value_length][value_bytes]
```
- Field names as tags (e.g. "Name", "Age") instead of single-byte keys
- No delimiters needed — length-prefix makes parsing unambiguous
- Self-describing: can decode without schema

**Record envelope**:
```
[1 byte version][1 byte flags][1 byte table_id][4 byte record_id LE][8 byte prev_version_offset LE][4 byte schema_version LE][4 byte payload_length LE][payload...][4 byte CRC32 LE]
```
- `flags & 0x01` = active
- `flags & 0x02` = has_prev_version
- `prev_version_offset` = 0 if no previous version (enables on-disk version chain)
- `schema_version` links record to a schema version in the table catalog

**Files to change**:
- `embeddbcore/encoding.go`: Remove `ValueStartMarker`/`ValueEndMarker`, add TLV encode/decode
- `embeddbcore/field_offsets.go`: Replace `Key byte` with `Name string` in `FieldOffset`, remove `Hash` (use schema version instead)
- `table_api.go`: Rewrite `encodeRecord`/`decodeRecord` to use TLV format
- `main.go`: Update record header constants and migration logic

### 1.2 Type-Aware Index Key Encoding
Replace `fmt.Sprintf("%v", value)` with deterministic byte encoding that preserves sort order.

**Encoding rules**:
| Type | Prefix | Encoding | Sort order |
|------|--------|----------|------------|
| uint/int | 0x01 | 8 bytes big-endian (flip sign bit for signed) | numeric |
| string | 0x02 | uvarint len + UTF-8 bytes | lexicographic |
| float | 0x03 | 8 bytes IEEE754 big-endian (flip sign+exponent for sort) | numeric |
| bool | 0x04 | 1 byte (0/1) | false < true |
| time | 0x05 | 8 bytes UnixNano big-endian | chronological |

**Files to create**:
- `key_encoding.go`: New file with `EncodeIndexKey`/`DecodeIndexKey`

### 1.3 B+ Tree with Byte-Size Split Threshold
Replace the existing B-tree with a proper B+ tree.

**Key changes from current**:
- Split when serialized cell data exceeds `(PageSize - headerOverhead) / 2`, not at fixed key count
- Values only in leaf nodes; internal nodes store only keys + child page numbers
- Leaf linked-list for O(k) range scans
- Copy-on-write pages for transaction safety
- CRC32 per page
- Node count stored in root page (O(1) instead of O(n) scan on open)
- Rebalancing on delete (merge/borrow when below min occupancy)

**Page format** (4096 bytes):
```
[1 byte page_type][1 byte flags][2 byte key_count][2 byte free_space_offset]
[4 byte prev_leaf_page][4 byte next_leaf_page]  // leaves only
[4 byte CRC32 at offset 4092]
```

**Cell format** (leaf):
```
[uvarint key_len][key_bytes][8 byte value LE]
```

**Cell format** (internal):
```
[uvarint key_len][key_bytes][4 byte left_child_page LE]
```

**Files to create**:
- `btree.go`: Complete rewrite as B+ tree with CoW pages

## Phase 2: Storage Layer

### 2.1 Persistent Free List
Persist the allocator's free list to disk so freed space is reusable across restarts.

**Format**: Written during `Sync()`/`Close()`, read during `Open()`.
```
[4 byte block_count][4 byte bitmap_size_pages]
[bitmap: 1 bit per page, 1=free]
[sorted free blocks: 8 byte offset + 8 byte length each]
[4 byte CRC32]
```

**Files to change**:
- `storage.go`: Add `Save()`/`Load()` methods for free list persistence

### 2.2 Separate Allocation Regions
Two allocators: one for index pages (4096-byte aligned), one for records (variable size).

**Files to change**:
- `main.go`: Add `indexAlloc` and `recordAlloc` to `database` struct
- `storage.go`: `allocator` unchanged, just used twice

### 2.3 Crash-Safe File Operations
- All writes go through mmap (copy to mapped region)
- `Sync()` calls `region.Sync()` to flush
- No mixing of `file.WriteAt` and mmap writes

**Files to change**:
- `main.go`: Remove `readAt`/`writeAt` fallback to `file.ReadAt`/`file.WriteAt` — always use mmap

## Phase 3: Write-Ahead Log

### 3.1 WAL Format
Append-only log written before any main-file mutation.

**Frame format**:
```
[4 byte magic 0xEDB1A111][8 byte transaction_id][1 byte frame_type]
[uvarint payload_len][payload_bytes][4 byte CRC32]
```

**Frame types**:
- `0x01` INSERT: table_id + record_id + TLV payload
- `0x02` UPDATE: table_id + record_id + old_offset + TLV payload
- `0x03` DELETE: table_id + record_id + old_offset
- `0x04` INDEX_PAGE: page_number + page_data (4092 bytes)
- `0x05` CATALOG: full catalog payload
- `0xFF` COMMIT: transaction_id only

### 3.2 WAL Lifecycle
1. On `Begin()`: record current WAL offset
2. On each mutation: write frame to WAL, `fsync`
3. On `Commit()`: write COMMIT frame, `fsync`, then apply to main file
4. On `Open()`: if WAL has uncommitted frames, replay them
5. Checkpoint: after `Sync()`, truncate WAL

**Files to create**:
- `wal.go`: WAL writer, reader, replay

## Phase 4: Transactions v2

### 4.1 Snapshot Isolation
- `Begin()` snapshots the B+ tree root offset (O(1), not O(n))
- Readers at snapshot T see only records committed before T
- Single writer, multiple concurrent readers
- Writer holds exclusive lock only during commit, not entire transaction

### 4.2 Copy-on-Write B+tree Pages
On write:
1. Allocate new page
2. Write modified data to new page
3. Propagate new page numbers up to root
4. On commit: atomically update root pointer (single 8-byte write)
5. On rollback: just discard new pages (old root still valid)

**Files to change**:
- `btree.go`: CoW page allocation, root pointer atomic swap
- `main.go`: `Transaction` struct uses root snapshot instead of full index copy

### 4.3 Rollback That Works
- Since CoW pages are never modified in-place, rollback is trivial
- Just discard all pages allocated during the transaction
- Restore root pointer to snapshot value
- Record data on disk from aborted transactions is harmless (active flag = 0)

## Phase 5: Schema & Migration v2

### 5.1 Schema Version in Table Catalog
Each table has a monotonically increasing `schema_version` (uint32). Migrations bump this version. Old records carry their schema version in the header.

### 5.2 Field-Name-Based Record Format
TLV fields use `string` field names (from struct tags), not single-byte keys. This makes:
- Migration robust: fields matched by name, not position
- Format self-describing: can read without schema
- No key-assignment instability when fields are added/removed/reordered

### 5.3 Lazy Migration
When a record's schema_version doesn't match the current table version, migrate on read:
1. Read old record bytes
2. Decode using old schema (or best-effort by field name)
3. Apply user-provided migration function: `func(old V1) V2`
4. Write new record with current schema_version
5. Update index to point to new record
6. Deactivate old record

If no migration function is provided, best-effort field-by-name match happens automatically (like current behavior, but by name instead of key byte).

### 5.4 Eager Migration (Optional)
`MigrateTable()` still available as a batch operation for when you want to upgrade all records at once.

**Files to change**:
- `table_api.go`: `Use[T]()` detects schema version mismatch, triggers lazy migration
- `main.go`: Remove `migrateTable`'s byte-copy approach, use TLV decode/re-encode

## Phase 6: Index Persistence

### 6.1 Persisted Secondary Indexes
Secondary indexes stored as B+ trees with root offsets in the table catalog. Created once, maintained incrementally. No full rebuild on `Use[T]()`.

**Table catalog format**:
```
[uint32 table_count]
For each table:
  [uvarint name_len][name][uint8 table_id][uint32 schema_version]
  [uvarint hash_len][hash][uint32 record_count][uint32 next_record_id]
  [uint64 pk_index_root_page]
  [uint16 secondary_index_count]
  For each secondary index:
    [uvarint field_name_len][field_name][uint64 root_page][uint8 unique_flag]
  [uint64 version_catalog_offset]
```

### 6.2 Uniqueness Enforcement
On `Insert`/`Update`, check secondary index before committing. If unique flag is set and key already exists, return error.

**Files to change**:
- `table_api.go`: `CreateIndex` creates B+ tree index, persists root in catalog
- `table_api.go`: `Insert`/`Update` check unique constraints

## Phase 7: Safe Vacuum

### 7.1 Atomic File Swap
Instead of rename-over (which has a data-loss window):

1. Write new file to `<filename>.new`
2. `fsync` new file
3. `os.Rename(filename, filename + ".bak")`
4. `os.Rename(filename + ".new", filename)`
5. `os.Rename(filename + ".bak", filename + ".old")` (keep one backup)
6. Re-open, remap
7. On next `Open()`, if `.new` exists: it was mid-swap, complete it
8. If `.old` exists: previous vacuum succeeded, clean up

**Files to change**:
- `main.go`: Rewrite `Vacuum()` with safe swap

## Phase 7: Updated API Surface

### Public API (unchanged semantics, improved internals)
```go
db, err := embeddb.Open("path.db", embeddb.OpenOptions{
    Migrate: true,           // enable auto-migration
    AutoIndex: true,         // auto-create indexes for tagged fields
    WAL: true,               // enable write-ahead log
    PageSize: 4096,          // B+ tree page size
})

tbl, err := embeddb.Use[User](db, "users",
    embeddb.UseOptions{
        MaxVersions: 5,       // keep last 5 versions
        MigrateFunc: embeddb.AutoMigrate[V1,V2](),  // or custom func
    },
)

// CRUD (same as v1)
id, err := tbl.Insert(&User{Name: "Alice"})
record, err := tbl.Get(id)
err = tbl.Update(id, &User{Name: "Alice v2"})
err = tbl.Delete(id)

// Versioning (same as v1)
versions, err := tbl.ListVersions(id)
old, err := tbl.GetVersion(id, 2)

// Transactions (same API, but now with proper rollback)
err = db.Begin()
// ... operations ...
err = db.Commit()  // or db.Rollback()

// Indexes (now persisted)
err = tbl.CreateIndex("Email")
err = tbl.DropIndex("Email")
err = tbl.CreateIndex("Name")  // non-unique by default
err = tbl.CreateIndex("Email", embeddb.IndexUnique)  // unique constraint
results, err := tbl.Query("Name", "Alice")

// Vacuum (same API, safer internals)
err = db.Vacuum()

// Close (auto-checkpoints WAL)
err = db.Close()
```

## Execution Order

| Step | Description | Depends on | Estimated effort |
|------|-------------|------------|------------------|
| 1 | TLV encoding in embeddbcore | Nothing | Medium |
| 2 | Type-aware index key encoding | Nothing | Small |
| 3 | B+ tree rewrite with byte-size splits | Nothing | Large |
| 4 | Record format v2 (new envelope + TLV) | Step 1 | Large |
| 5 | Persistent free list | Step 4 | Medium |
| 6 | Separate allocators | Step 5 | Small |
| 7 | Crash-safe mmap-only I/O | Step 4 | Medium |
| 8 | WAL | Steps 3, 4 | Large |
| 9 | CoW B+ tree pages | Step 3 | Medium |
| 10 | Snapshot isolation transactions | Steps 8, 9 | Large |
| 11 | Schema version + field-name TLV | Step 4 | Medium |
| 12 | Lazy migration | Step 11 | Medium |
| 13 | Persisted secondary indexes | Steps 3, 5 | Medium |
| 14 | Uniqueness enforcement | Step 13 | Small |
| 15 | Safe vacuum | Step 7 | Medium |
| 16 | Table API v2 (glue everything together) | All above | Large |
| 17 | Full test suite | Step 16 | Large |

Starting with **Step 1** (TLV encoding) since it's the foundation everything else builds on.