# Plan: Add StorageFile Mode (No Mmap)

## Overview
Add a `StorageMode` flag to `OpenOptions` that allows running embeddb without memory-mapped files, avoiding mmap overhead when desired.

The codebase already has fallback paths in `readAt`/`writeAt` when `region` is nil — they fall back to `file.ReadAt`/`file.WriteAt`. The main blocker is the **B-tree**, which directly uses `unsafe.Slice` on the mmap pointer for page access.

## Changes by File

### 1. `table_api.go` — Add storage mode flag
- Add `type StorageMode int` with `StorageMmap` (default=0) and `StorageFile`
- Add `StorageMode` field to `OpenOptions`
- Add `storageMode` field to `DB` struct
- Pass it through to `openDatabase`

### 2. `database.go` — Conditional mmap initialization
- Update `openDatabase` signature to accept storage mode
- When `StorageFile`: skip `ensureRegion()` entirely, leave `region` as nil
- When `StorageMmap`: current behavior (create mmap region)
- `readAt`/`writeAt` already handle nil region — no changes needed
- `readAtFn` already handles nil region — no changes needed
- `flush()` already guards `region.Sync()` with nil check — no changes needed
- `Close()` already guards `region.Unmap()` with nil check — no changes needed
- `autoSync()` already guards `region.Sync()` with nil check — no changes needed

### 3. `btree.go` — Abstract page access (main work)
Four methods directly use mmap pointers and need branching:

- **`pageData(offset)`** → `pageData(offset) []byte`:
  - When mmap: current zero-copy slice into mmap
  - When file mode: use `db.readAt` into a pooled buffer, return the copy

- **`newNode()`**:
  - When mmap: current behavior
  - When file mode: use `db.writeAt` to write the initialized page buffer

- **`writeNode()`**:
  - When mmap: current behavior via `pageData`
  - When file mode: use `db.writeAt` with serialized data

- **`readNode()`**:
  - When mmap: current behavior (reads from mmap, CRC fallback to file)
  - When file mode: use `db.readAt` directly (CRC check still applies)

- **`readCount()`**:
  - When mmap: current behavior via `pageData`
  - When file mode: use `db.readAt` into a small buffer

- **`Sync()`**: Already guards with nil check — no changes needed

### 4. `vacuum.go` — Guard mmap operations
- Already has nil checks for `region.Sync()` and `region.Unmap()`
- `ensureRegion` calls need to be no-ops when region is nil (add early return)

## Usage Example
```go
db, _ := embeddb.Open("my.db", embeddb.OpenOptions{
    StorageMode: embeddb.StorageFile, // No mmap overhead
})
```

## Buffer Pooling for File Mode
The B-tree already uses `pageBufPool` (4096-byte buffers). In file mode, `pageData` will allocate from this pool, read via `db.readAt`, and return the buffer. Callers must not hold references across operations (current code already copies data out, so this is safe).

## Default Behavior
`StorageMmap` is the default (zero value), maintaining full backward compatibility.
