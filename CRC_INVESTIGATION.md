The torture test (`torture/main.go`) with `-workers 3+` produces two types of errors:

1. **CRC mismatches** on B-tree pages (e.g. `btree page 14652477: CRC mismatch (stored=66da5c6e computed=05e7f765)`)
2. **"record not found"** on updates (e.g. `Update(135): record not found`)

These do not appear with 1-2 workers (zero errors). Rate: ~1/20K ops with 4 workers.

Error characteristics:
- Same page offset gets multiple CRC errors, with different stored CRCs over time
- Stored CRC eventually changes to ASCII text (e.g. `62616261` = `"abab"`) — meaning record data overwrote the B-tree page
- Same page's computed CRC varies across reads — meaning the mmap data keeps changing between reads

---

## 2. What Has Been Ruled Out

### 2.1 Go-level data races
- Full test suite passes `go test -race` with zero warnings
- Fixed races found: unguarded `bt.hits`/`bt.misses` counters, unsynchronized `bt.rootOff` in `SetRootOffset`, unsynchronized `entry.RecordCount` in `Count()`

### 2.2 Allocator double-allocation
- Added `allocator.debugMode` that panics if `Allocate()` returns a duplicate or overlapping offset
- Ran 25s torture test with debug enabled — zero panics
- Conclusion: the allocator never returns the same offset twice

### 2.3 mmap library coherence
- Wrote `TestConcurrentReadWriteStress` — writers and readers to separate pages (non-overlapping). 10s run, zero corruption.
- Wrote `TestConcurrentReadWriteOverlapping` — each writer owns specific pages, readers scan all pages verifying ownership via magic footers. 10x15s runs, zero corruption.
- Conclusion: mmap at the library level does not exhibit data bleeding between pages

### 2.4 Region lock semantics
- Tried changing `writeAt` from `r.RLock()` to `r.WriteLock()` — caused SIGBUS crashes (region getting unmapped between lock acquisition and copy)
- Reverted to `r.RLock()` (original behavior)

### 2.5 Single-worker correctness
- 1 worker: 0 errors
- 2 workers: 0 errors
- Issue only appears at 3+ concurrent goroutines

---

## 3. Lock Hierarchy (Documented)

```
db.mu  (database) → bt.mu  (B-tree operations) → cacheMu  (page cache)
```

- `db.mu.Lock()` serializes all writes (Insert, Update, Delete, InsertManyBulk)
- `bt.mu.RLock()` protects all reads (Get, Scan, Query) — shared, allows concurrent reads
- `bt.mu.Lock()` protects all B-tree writes — exclusive
- `Region.mu.RLock()` protects mmap region from being resized during access
- `Region.mu.Lock()` / `WriteLock()` for region resize operations
- `cacheMu` protects the B-tree page cache and ring buffer

---

## 4. Possible Root Causes (Not Yet Ruled Out)

### 4.1 B-tree node structure corruption when reading cached nodes
- `readNode` returns a `*BTreeNode` from the cache (under `bt.mu.RLock()`)
- If a B-tree write (under `bt.mu.Lock()`) later modifies the same node struct's fields, the cached pointer now points to mutated data
- The write path calls `cacheNode(node)` which updates the cache, but the cached pointer IS the same struct — the write mutates the struct that was already returned to a reader
- However: `bt.mu.Lock()` blocks `bt.mu.RLock()`, so a reader can't hold a cached pointer while a writer modifies the node. This should be safe.

### 4.2 B-tree `splitChild` correctness
- `splitChild` creates a new sibling node, redistributes keys, and writes both nodes back
- Potential issue: off-by-one in Children index assignment, wrong PrevLeaf/NextLeaf pointers
- Need to audit this function carefully

### 4.3 `BulkInsert` + concurrent `Get` interaction
- `InsertManyBulk` holds `db.mu.Lock()` but calls `BTree.BulkInsert` which acquires/releases `bt.mu.Lock()`
- Between `BulkInsert` (primary keys) and subsequent `BTree.Insert` calls (version/secondary keys), `bt.mu` is released
- A concurrent `Get` (under `bt.mu.RLock()`) reads a partially-populated B-tree
- Should be safe because primary keys are already in the tree, but secondary keys may be missing

### 4.4 Root page count field (`bt.count`)
- Removed redundant `bt.count` update from `readNode` in this session
- `bt.count` is now only updated by `Insert`/`Delete`/`BulkInsert`/`openBTree`
- Need to verify that `bt.count` stays correct across splits and that the count is properly written to the root page via `serializeNode`

### 4.5 Transaction rollback interaction
- `Rollback` restores allocator, clears cache, sets new root (now under `bt.mu.Lock()`)
- Between clearing cache and restoring allocator, a concurrent `Get` could theoretically read freed pages
- Mitigated by `SetRootOffset` now acquiring `bt.mu.Lock()` which waits for all readers

---

## 5. Investigation Plan

### Phase 1: Isolate the error type
**Run the torture test with transactions disabled** to determine if the bug is transaction-related.

Edit `torture/main.go` to skip `runTransaction`, or run with `-workers 3` and observe:
- If CRC errors still appear: bug is in B-tree concurrency, not transactions
- If only "record not found" disappears: the "record not found" bug is transaction-related
- If both disappear: the bug is transaction-related

### Phase 2: Add B-tree structural validation
Add periodic checks during B-tree operations to detect corruption at the point of occurrence:

```go
// In readNode, after decoding the page:
if node.Count < 0 || node.Count > MaxKeys {
    panic("corrupt node count")
}
// Verify Children offsets are within file bounds
for _, child := range node.Children {
    if child < FileHeaderSize {
        panic("corrupt child offset")
    }
}
```

### Phase 3: Trace the corrupted page
When a CRC error is detected, log:
- The page offset
- Which operation was running (Insert? Get? Scan?)
- The current `bt.rootOff`
- Whether the node was in the cache
- The data at the page offset (hex dump of first 64 bytes)

### Phase 4: Audit `splitChild`
- Write a standalone B-tree test that inserts keys from multiple "tables" (different prefixes) and verifies structural integrity after each insert
- Check that `PrevLeaf`/`NextLeaf` chains remain consistent
- Check that all keys remain findable by `Get`

### Phase 5: Write a minimal reproduction
Reduce the torture test to the minimal operations that reproduce the error:
- Remove secondary operations (Query, Scan, Filter, Pagination)
- Keep only Insert, Get, Update, BulkInsert for 4 workers
- Once repro is minimal, bisect which specific operations trigger the error

---

## 6. Commands

```bash
# Run torture test with 3 workers, 0 errors expected from code
go run torture/main.go -workers 3 -stats 1s -duration 30s

# Run full test suite with race detector
go test ./... -timeout 5m -skip TestStress10M -race

# Run mmap coherence tests
cd embeddbmmap && go test -run 'TestConcurrentReadWrite' -count=5 -v

# Run torture test with allocator debug (add db.EnableAllocatorDebug() before running)
go run torture/main.go -workers 4 -stats 1s -duration 30s
```
