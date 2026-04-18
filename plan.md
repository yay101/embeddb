# EmbedDB v1.0.0 — Improvement Plan

## Priority 1: Performance (High Impact)

### 1.1 B-Tree Range Scans for QueryRange
**Problem**: All range queries (`QueryRangeGreaterThan`, `QueryRangeLessThan`, `QueryRangeBetween`, `QueryNotEqual`, `QueryGreaterOrEqual`, `QueryLessOrEqual`, `QueryLike`, `QueryNotLike`) fall through to `filterByField()` — a full table scan deserializing every record. Benchmark shows `QueryRangeBetween` at **13.7ms** vs `Query` at **312µs** — a 44x gap.

**Fix**: Add `ScanRange(startPrefix, endPrefix []byte)` to `BTree` that walks the leaf chain starting from the first key ≥ `startPrefix` and stopping after keys exceed `endPrefix`. Then implement each range method to construct appropriate start/end bounds from the secondary key encoding and call `ScanRange` instead of `filterByField`.

**Example** for `QueryRangeGreaterThan("Age", 20)`:
- Start: `encodeSecondaryKeyPrefixWithValue(tableID, "Age", encodeIndexValue(21))`
- End: `encodeSecondaryKeyPrefix(tableID+1, "Age")` (or a sentinel)

**Files**: `btree.go` (add `ScanRange`), `table_api.go` (rewrite QueryRange* methods), `index.go` (add bound constructors)

**Expected result**: Range queries drop from O(n) to O(k+m) where k is result count and m is sibling leaf pages traversed. Should match `Query` performance (~300µs).

---

### 1.2 B-Tree Page Cache Improvements
**Problem**: Cache is a fixed 2048-entry ring buffer (8MB) with FIFO eviction, `sync.Mutex` (no read concurrency), root page gets no special treatment, and the unused `pagePool` sync.Pool wastes code.

**Fixes**:
- Replace `sync.Mutex` with `sync.RWMutex` for `cacheMu` — concurrent reads should not serialize
- Pin the root node in cache (never evict) — it's traversed on every operation
- Make cache size configurable via `OpenOptions.CachePages` (default 4096)
- LRU eviction instead of FIFO — use a doubly-linked list or container/heap
- Remove the unused `pagePool` at `btree.go:33-38`

**Files**: `btree.go`

---

### 1.3 Eliminate Double Allocation in serializeNode
**Problem**: `serializeNode()` allocates two `PageSize` buffers — one working buffer and one result copy (`btree.go:189,231`). Every node write wastes 4KB.

**Fix**: Return the working buffer directly. The caller (`writeNode`) already copies it to the mmap region.

**Files**: `btree.go:188-233`

---

### 1.4 Remove Pre-Insert Search in B-Tree
**Problem**: `Insert()` and `Put()` both call `searchUnlocked(key)` before the actual insert just to decide whether to increment `bt.count`. This is an extra full tree traversal per insert.

**Fix**: Track count during the insert itself. After `insertNonFull`, check if the key was newly added vs updated (return a bool). Alternatively, maintain a count delta and reconcile on flush.

**Files**: `btree.go:401-468`

---

### 1.5 Reduce Per-Record Allocations in Hot Paths
**Problem**: Every `readRecordAt` allocates 2-3 byte slices (header, record buffer, result). `ScanRecords().Next()` re-acquires `db.mu.RLock()` per record. `encodePrimaryKey`/`encodeSecondaryKey` allocate on every call.

**Fixes**:
- `Get()` already holds RLock — `readRecordAt` shouldn't re-lock internally
- Pool record buffers or use arena allocation for batch operations
- Use `sync.Pool` for `encodePrimaryKey` buffers (currently `index.go:17-23` allocates fresh each time)
- `ScanRecords` should read all records in a single RLock pass, not per-record Get()

**Files**: `table_api.go`, `index.go`, `main.go`

---

### 1.6 Allocator Mutex Holds Through Syscalls
**Problem**: `Allocate()` holds `a.mu.Lock()` through `file.Truncate` and `mmap.Resize` — both blocking syscalls. All concurrent writes serialize on this.

**Fix**: Pre-allocate the file in chunks (e.g., 64MB at a time). Only take the mutex for the fast path (bump offset). Slow path (growth) is rare and can be lock-free after reservation.

**Files**: `storage.go:66-123`

---

## Priority 2: Correctness & Robustness

### 2.1 Rebuild Secondary Indexes After Corruption Recovery
**Problem**: `rebuildIndexFromScan()` only rebuilds primary keys. Secondary and version entries are permanently lost. After reopening, `Query()` returns empty results and `ListVersions()` fails.

**Fix**: After inserting primary keys, scan each record's payload to extract indexed field values and re-insert secondary keys. For version entries, use the record header's `PrevVersionOff` to reconstruct the version chain.

**Files**: `main.go:439-566`

---

### 2.2 Transaction Rollback Doesn't Reclaim Space
**Problem**: On `Rollback()`, the B-tree root is reset and cache is cleared, but written record data and allocated B-tree pages remain in the file. The allocator's free list is never restored.

**Fix**: Snapshot the allocator state at `Begin()`. On `Rollback()`, restore it. Alternatively, use a WAL-based approach for true atomicity.

**Files**: `main.go:1085-1158`

---

### 2.3 Silent Error Swallowing in Delete
**Problem**: `readAt` errors are silently discarded in `Delete()` (`table_api.go:864-865`) and `DeleteMany()`. If the read fails, `hdr.RecordID` is 0, and secondary keys won't be cleaned up — orphaned index entries persist forever.

**Fix**: Return errors from `readAt` calls. If the read fails, skip the record but log a warning. Consider adding an `Err` return to `Delete`.

**Files**: `table_api.go:864-865`

---

### 2.4 Mmap Region Data Race
**Problem**: In `readAt()`/`writeAt()`, after releasing `currentRegion.RLock()` and calling `ensureRegion()`, the region is re-loaded. If another goroutine resizes the region between the reload and the copy, the offset could be stale.

**Fix**: Hold the region RLock through the entire read/write operation, or use atomic pointer swaps that guarantee the region doesn't change mid-operation.

**Files**: `main.go:248-277`

---

## Priority 3: Code Quality & Cleanup

### 3.1 Remove Dead Code
- `mapIndex` type and all methods (`main.go:24-108`) — unused
- `btreeMapIndex` and `btreeOffsetMapIndex` (`btree.go:964-1130`) — vestigial wrappers for deleted interfaces
- `decodeVersionValue`/`encodeVersionValue` (`index.go:85-99`) — never called
- `shrinkRegion` (`main.go:231-237`) — no-op
- `pagePool` (`btree.go:33-38`) — defined but never used
- Entire `wal.go` — defines types but never integrated

**Files**: `main.go`, `btree.go`, `index.go`, `wal.go`

---

### 3.2 Split table_api.go (2064 lines) Into Focused Files
Current file mixes concerns:
- `table_crud.go` — Insert, Get, Update, Delete, Upsert
- `table_query.go` — Query, QueryRange*, filterByField
- `table_index.go` — CreateIndex, DropIndex, insertSecondaryKeys, deleteSecondaryKeys, explicitIndexes
- `scanner.go` — ScanRecords, Scanner type, Next/Record/Close
- `pagination.go` — Paged result types and pagination logic
- `types.go` — normalizePK, compareValues, valueToIndexKey, helper functions

---

### 3.3 Split main.go (1288 lines) Into Focused Files
- `database.go` — openDatabase, Close, load, flush, ensureRegion, readAt, writeAt
- `vacuum.go` — Vacuum
- `transaction.go` — Begin, Commit, Rollback
- `migration.go` — migrateTable
- `allocator.go` — already in storage.go, but mapIndex should be removed

---

### 3.4 Secondary Key Encoding: Use RecordID Instead of RecordOffset
**Problem**: `encodeSecondaryKey` embeds `recordOffset` as a suffix. When Vacuum compacts the file, every secondary key must be reconstructed with new offsets. This is O(n) and requires a full index rebuild.

**Fix**: Use `recordID` (uint32, already in the record header) instead of `recordOffset`. Then secondary keys become stable across Vacuum operations. To look up a record, use the primary key index: `secondaryKey → recordID → primaryKey → offset`.

This requires either:
- Storing `recordID → primaryKey` mapping somewhere, or
- Encoding `tableID + recordID` in the secondary key and scanning primary keys for that recordID

**Trade-off**: Adds an extra primary key lookup per secondary query, but eliminates the need to rebuild secondary keys during Vacuum.

**Files**: `index.go`, `main.go` (Vacuum), `table_api.go`

---

## Priority 4: Future Features

### 4.1 Write-Ahead Log (WAL)
**Problem**: No durability guarantee. A crash between `Insert` and `Sync` loses data. The `wal.go` placeholder exists but is never integrated.

**Fix**: 
- On `Open`, create WAL file
- Before each mutation, append a WAL frame (operation type, key, value)
- On `Sync`, flush WAL then flush main data
- On recovery, replay WAL frames

**Files**: `wal.go`, `main.go`

---

### 4.2 B-Tree Prefix Range Scan Method
**Problem**: Currently `Scan()` iterates all leaf nodes. No method for bounded iteration from a start key to an end key.

**Fix**: Add `ScanRange(startKey, endKey []byte, fn func(key []byte, value uint64) bool)` to `BTree`:
1. Search for the first key ≥ `startKey`
2. Walk leaf chain until keys exceed `endKey`
3. Call `fn` for each entry

This is needed for Priority 1.1 (range queries) but also useful for prefix queries, pagination, and cursor-based iteration.

**Files**: `btree.go`

---

### 4.3 Configurable Cache Size
**Problem**: B-tree page cache is hardcoded to 2048 entries regardless of database size or available memory.

**Fix**: Add `CachePages int` to `OpenOptions`. Default to 4096. Apply in `openBTree`.

**Files**: `table_api.go` (OpenOptions), `btree.go` (openBTree)

---

### 4.4 Batch Operations
**Problem**: `InsertMany`, `UpdateMany`, `DeleteMany` all hold the write lock for the entire batch and perform individual B-tree operations. No batching at the B-tree level.

**Fix**: Add `BTree.BatchInsert(entries []KeyValue)` that amortizes lock acquisition and page splits. Also consider `Table.BatchInsert(records []*T)` that does a single lock acquisition + batch B-tree mutation.

**Files**: `btree.go`, `table_api.go`

---

### 4.5 CRC Verification Toggle
**Problem**: Every B-tree page read verifies CRC (`btree.go:304-319`). This adds CPU overhead on every cache miss during normal operation. CRC should only be verified on recovery.

**Fix**: Add `VerifyCRC bool` to `OpenOptions`. Only check on `rebuildIndexFromScan` path or explicit `Verify()` call.

**Files**: `btree.go`, `table_api.go`