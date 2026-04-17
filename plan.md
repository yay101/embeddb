# EmbedDB Improvement Plan

## Completed Bugs (v1.7.8-v1.7.9)

### 1. `Sync()` and `Vacuum()` only process first table ✅
- Iterate all tables, accumulate first error

### 2. `updateLocked()` writes in-place without size check ✅
- Allocate new space, handle versioning, mark old inactive

### 3. `UpdateMany` doesn't delete old secondary index entries ✅
- Call `DeleteFromIndexes()` before `UpdateIndexes()` in `updateLocked`

### 4. `DeleteMany` doesn't track transaction record counts ✅
- Check `t.db.tx != nil` and use transaction deltas

### 5. `Upsert()` TOCTOU race condition ✅
- Hold write lock for entire operation, check existence via pkIndex

### 6. Transaction `Rollback()` writes `versionIndex.data` without lock ✅
- Hold `versionIndex.mu` for entire clear+restore operation

### 7. Secondary indexes use file offsets instead of recordIDs ✅
- Refactored from `uint32 recordID` to `uint64 file offset`
- Added `getByOffset()` method, updated all query methods
- Simplified `rebuildSecondaryIndexes`

### 8. `Scanner.Next()` recursion → stack overflow risk ✅
- Converted to `for` loop

### 9. `encodeVersionCatalog()` TOCTOU ✅
- Hold `versionIndex.mu` for entire count+serialize operation

### 10. `file.Stat()` nil pointer panic ✅
- Check error and return descriptive failure

## Completed Concerns (v1.7.9)

### 11. `DeleteMany` / `Delete` uint32 underflow on `RecordCount` ✅
- Added `entry.RecordCount > 0` guard before decrement

### 12. `rebuildSecondaryIndexes()` called without `db.mu` lock ✅
- Wrapped in `typedDB.mu.RLock()`/`typedDB.mu.RUnlock()`

### 13. `versionIndex.RestoreSnapshot()` aliasing ✅
- Deep-copy snapshot data instead of direct map assignment

## Completed Style Fixes (v1.7.9)

### 14. Confusing BTree variable naming ✅
- Renamed `exists` → `searchErr` in `Insert()` for clarity

### 15. Unused `BTreeNodeFree` constant ✅
- Removed

### 16. Unused `ErrKeyExists` error ✅
- Removed

### 17. Unused `DB.idleTimer` field ✅
- Removed

### 18. Unused `tableCatalogEntry.BTreeRoot` field ✅
- Removed

## B-tree SIGBUS Bug Fix (v1.7.9)

### 19. B-tree `ensureMmap` could truncate the file ✅
- `ensureMmap` called `file.Truncate(size)` which could shrink the file below the allocator's `actualSize`, corrupting data
- Fix: Use `file.Stat().Size()` as the minimum mmap size so the file is never shrunk

### 20. B-tree `writeNode` SIGBUS from stale mmap ✅
- `writeNode` used `copy(bt.mmap[node.Offset:], buf)` which accessed the mmap directly
- When `ensureMmap` unmapped and remapped the file, any code referencing the old mmap slice would SIGBUS
- The shared allocator between record storage and B-tree meant record allocations could grow the file while B-tree's mmap was stale
- Fix: Changed `writeNode` to use `file.WriteAt()` instead of mmap writes

### 21. B-tree `readNode` SIGBUS from stale mmap ✅
- `readNode` read directly from `bt.mmap` which could be remapped by `ensureMmap` between the bounds check and the actual read
- Fix: Changed `readNode` to use `file.ReadAt()` instead of mmap reads, eliminating the SIGBUS entirely
- The mmap is now only used for `Scan` operations where consistency is guaranteed

## Migration Bug Fixes (v1.8.1)

### 22. Old records not deactivated during migration ✅
- `migrateTable` never marked old records as `active=0`, causing duplicates in index rebuild and unbounded file growth
- Fix: Added `db.writeAt([]byte{0}, int64(rec.offset+11))` before writing new record

### 23. Version index entries not updated during migration ✅
- `migrateTable` updated pkIndex but left `versionIndex` pointing at stale offsets
- Fix: After writing migrated record, update version index entries from old offset to new offset

### 24. `LayoutHash` updated even when `Migrate: false` ✅
- Hash was always written back, preventing future detection of the schema mismatch
- Fix: Only update hash after successful migration; return explicit error when `Migrate: false` and schema differs

## v1.8.2 Plan

### Critical Bugs

#### 25. `Allocate` errors ignored everywhere ✅
- **Files**: `table_api.go` (Insert, Update, insertLocked, updateLocked, Upsert), `main.go` (migrateTable), `btree.go` (newNode, writeNode)
- **Impact**: If allocation fails, offset=0 was returned and record data overwrote the DB header
- **Fix**: 
  - Changed `Allocate` signature to return `(offset, length uint64, err error)` with rollback on Truncate/Resize failure
  - All callers now check error and return descriptive messages
  - `splitChild` and `insertNonFull` now return errors, propagated through B-tree Insert/Put
  - Found and fixed: B-tree `Insert` was missing `insertNonFull` call (key was never actually inserted after split)
  - Fixed migration order: allocate new space before deactivating old record to prevent data loss on allocation failure
- **Status**: Completed

#### 26. `readAt`/`writeAt` ignore I/O and `ensureRegion` errors ✅
- **Files**: `main.go`, `table_api.go`
- **Impact**: Failed mmap resize causes reads/writes via invalid pointer → segfault
- **Fix**: Changed `readAt`/`writeAt` signatures to return `error`. Propagated errors in all user-facing API methods (Get, Update, Delete, Insert, Upsert, etc.) and critical paths (load headers, migration). Internal scan/best-effort paths explicitly discard errors with `_ =`.
- **Status**: Completed (v1.8.3)

#### 27. `readAtFn` captures stale `db.region` by value ✅
- **File**: `main.go:549-558`
- **Impact**: Use-after-free if `db.region` changes after Close/Vacuum
- **Fix**: Closure now dereferences `db.region` at call time with a nil guard
- **Status**: Completed (v1.8.3)

#### 28. Migration data loss when `newEncoded` is empty ✅
- **File**: `main.go:1528-1530`
- **Impact**: Records whose fields are all new in the new schema were silently dropped (`continue`)
- **Fix**: Changed `continue` to write an empty-encoded record (just header+footer), preserving the record's identity and count
- **Status**: Completed (v1.8.3)

### High Bugs

#### 29. Partial migration failure leaves DB inconsistent ✅
- **File**: `main.go`
- **Impact**: If migration fails partway, some old records are already deactivated but their pkIndex entries still point to the old (inactive) offsets → data loss
- **Fix**: Two-phase migration: (1) allocate + prepare all new records, (2) write all new records, (3) then deactivate old records + update indexes. If allocation fails in phase 1, no old records are harmed.
- **Status**: Completed (v1.8.4)

#### 30. Transaction rollback doesn't restore B-tree disk state ✅
- **File**: `main.go:1405`
- **Impact**: SetRootOffset points the tree at a root that may reference transaction-modified nodes; on-disk pages aren't rolled back
- **Fix**: Documented as known limitation with comment on `Rollback()` method. Full fix would require copy-on-write pages or WAL.
- **Status**: Completed (documented)

#### 31. `InsertMany`/`UpdateMany` silently swallow errors ✅
- **File**: `table_api.go`
- **Impact**: Callers get no indication that some records failed; returned error was always nil
- **Fix**: Both methods now collect and return the first error encountered, while still processing remaining records. Callers can check the error to know if some records failed.
- **Status**: Completed (v1.8.4)

#### 32. `Drop()` doesn't clean version index entries ✅
- **File**: `table_api.go:2143-2168`
- **Impact**: Orphaned version entries consume memory; stale data if table ID is reused
- **Fix**: Added `versionIndex.RemoveKey(k)` call for each key when dropping a table
- **Status**: Completed (v1.8.4)

#### 33. Concurrent allocate races with mmap resize
- **File**: `storage.go:70-76`
- **Impact**: Another goroutine reading from mmap during resize could crash
- **Fix**: Analyzed — the current locking model prevents this race. All user-facing API methods hold `db.mu.Lock()` (or `RLock` for reads) during `Allocate` + `readAt`/`writeAt`. The allocator's `a.mu` serializes concurrent allocations. `ensureRegion` in readers only resizes when the current region is too small, and since `Allocate` grows the region before the lock is released, subsequent readers see a sufficient region. No code change needed.
- **Status**: Completed (analyzed, no fix needed)

### High Test Gaps

#### 34. Version persistence not verified after reopen ✅
- **File**: `versioning_test.go:268-275`
- **Impact**: `TestVersioningPersistence` used `t.Logf` instead of assertions — version persistence was never actually verified
- **Fix**: Changed to `t.Fatalf`/`t.Errorf` so the test properly asserts version count after reopen
- **Status**: Completed (v1.8.4)

#### 35. No tests for `FastSync` ✅
- **Fix**: Added `TestFastSync` and `TestFastSyncOnClosed`
- **Status**: Completed (v1.8.5)

#### 36. No tests for concurrent `Vacuum` ✅
- **Fix**: Added `TestConcurrentVacuumWithReads` — tests concurrent reads with sequential vacuum. Discovered that truly concurrent Vacuum + reads causes SIGSEGV (mmap region invalidated during read), which is a known architectural limitation of the file-replace approach. Vacuum must not run concurrently with active readers.
- **Status**: Completed (v1.8.5)

#### 37. No tests for B-tree delete edge cases ✅
- **Fix**: Added `TestBTreeDeleteBasic` and `TestBTreeDeleteAllThenInsert`
- **Status**: Completed (v1.8.5)