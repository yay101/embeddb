# EmbedDB v0.6.0 Optimization Plan

## Overview
This document outlines the optimization plan for EmbedDB v0.6.0, focusing on improving performance and reducing memory usage while maintaining correctness.

## Priority Matrix

| Priority | Area | Issue | Expected Impact | Status |
|----------|------|-------|----------------|--------|
| P0 | Lock Contention | Global `regionCatalogMu` single lock | High concurrency improvement | **DONE** |
| P0 | Lock Upgrade | RLock→Lock pattern in Find() | Reduced starvation | Pending |
| P0 | Hot Path Allocations | Buffer allocations in Insert | Fewer allocations | **DONE** |
| P0 | ScanNext Memory | Entire file read into memory | 50% less memory | Pending |
| P1 | Field Lookup | O(n) linear search per insert | 2-5x faster indexing | **DONE** |
| P1 | String Hashing | fmt.Sprintf in hashKey() | Faster queries | Pending |
| P1 | Result Preallocation | Missing capacity hints | Fewer allocations | Pending |
| P2 | Tokenization Cache | Repeated string tokenization | Fewer allocations | Pending |
| P2 | Buffer Clearing | Byte-by-byte zero | Faster cleanup | Pending |
| P2 | applyWrites Slices | No preallocation | Fewer allocations | Pending |

---

## P0: Critical Optimizations

### 1. Lock Contention - Global `regionCatalogMu`
**File:** `btree_region_page_store.go:32`

**Issue:** Single global `sync.Mutex` protects all region catalog operations across ALL indexes. Under concurrent load, all index operations serialize on this lock.

**Current Code:**
```go
var regionCatalogMu sync.Mutex  // Single global lock
```

**Solution:** Replace with per-file locking. Each `regionBackedBTreePageStore` should have its own lock, with the global lock only used for catalog-level operations (creation, growth).

**Implementation:**
- Add `mu sync.Mutex` to `regionBackedBTreePageStore` struct
- Use instance lock for read/write operations
- Keep global lock only for catalog entry creation/growth

---

### 2. Lock Upgrade Pattern
**File:** `btree_index.go:949-961`

**Issue:** `Find()` acquires RLock, then releases it and acquires Lock when `pendingWrites > 0`. This causes writer starvation and lock convoy issues.

**Solution:** Restructure to avoid upgrade:
- Check pending writes length BEFORE acquiring lock
- Or use atomic flag to indicate pending writes
- Or use try-lock pattern

---

### 3. Hot Path Allocations
**Files:** `dbops.go:617,636`, `table.go:49,63`

**Issue:** Buffer allocations on every Insert:
```go
headerBytes := make([]byte, 12)  // Allocated every insert
completeRecord := make([]byte, 0, ...)  // Append-heavy
```

**Solution:** Pre-allocate exact size:
```go
completeRecord := make([]byte, 12+len(encoded)+2)
```

---

### 4. ScanNext Memory Usage
**File:** `mmap.go:85`

**Issue:** Reads entire file into memory on every scan:
```go
data := make([]byte, db.mfile.Len())  // Entire file
```

**Solution:** Use sliding window or range reads:
- Read in chunks (e.g., 1MB at a time)
- Reuse buffer across iterations
- Use mmap directly with offset

---

## P1: High-Priority Optimizations

### 5. Field Offset Linear Search
**File:** `indexing.go:322-330`

**Issue:** Every `InsertIntoIndexes` does O(n) linear search through `FieldOffsets`:
```go
for _, offset := range im.layout.FieldOffsets {
    if strings.EqualFold(offset.Name, fieldName) { ... }
}
```

**Solution:** Pre-build `fieldName→FieldOffset` map at initialization.

---

### 6. String Hashing
**File:** `btree_index.go:1884,1892`

**Issue:** `fmt.Sprintf()` used in hot path for type conversion:
```go
fmt.Sprintf("%v", key)  // Slow
```

**Solution:** Direct type conversion:
```go
switch k := key.(type) {
case string: str = k
case int: str = strconv.FormatInt(k, 10)
case bool: if k { str = "1" } else { str = "0" }
// ...
}
```

---

### 7. Result Slice Preallocation
**File:** `btree_index.go:1135`

**Issue:** `searchRange()` doesn't pre-allocate results:
```go
var results []uint32  // No capacity hint
```

**Solution:** Estimate and pre-allocate:
```go
estimated := int(node.KeyCount) * 4
results := make([]uint32, 0, estimated)
```

---

## P2: Medium-Priority Optimizations

### 8. Tokenization Cache
**File:** `btree_index.go:79-86`

**Issue:** Same string tokenized repeatedly on insert/query.

**Solution:** Add LRU cache for tokenized strings.

---

### 9. Buffer Clearing
**File:** `btree_index.go:569`

**Issue:** Byte-by-byte loop to clear buffers:
```go
for i := range buf { buf[i] = 0 }
```

**Solution:** Use `clear()` builtin (Go 1.21+).

---

### 10. applyWrites Slice Preallocation
**File:** `btree_index.go:2059-2061`

**Issue:** Temporary slices not pre-allocated:
```go
var valuesToAdd []uint32
var valuesToRemove []uint32
```

**Solution:**
```go
valuesToAdd := make([]uint32, 0, 64)
valuesToRemove := make([]uint32, 0, 64)
```

**Status:** ✅ Done

---

## Implementation Order

1. **Lock Contention** - Most impactful for concurrent workloads
2. **Hot Path Allocations** - High impact, low risk
3. **ScanNext Memory** - Significant memory improvement
4. **Field Offset Cache** - High impact on indexing
5. **String Hashing** - Medium impact, straightforward
6. **Result Preallocation** - Low risk, incremental gain
7. **Tokenization Cache** - Medium impact, moderate complexity
8. **Buffer Clearing** - Low impact, easy fix
9. **applyWrites Slices** - Low impact, easy fix
10. **Lock Upgrade** - Complex, defer if needed

---

## Testing

After each optimization:
1. Run `go test -short ./...` to verify correctness
2. Run benchmarks: `go test -bench=. -benchmem`
3. Compare against baseline metrics

### Baseline Metrics (v0.6.0)
- Insert: ~140k ns/op
- QueryInt: ~170k ns/op
- Peak Heap (inserts): ~9.5 MB
- Peak Heap (queries): ~2.5 MB

### Target Metrics
- Insert: <100k ns/op (30% improvement)
- QueryInt: <150k ns/op (15% improvement)
- Peak Heap: <8 MB during inserts

---

## Completed Optimizations

### Optimization #1: Lock Contention (P0)
- **File**: `btree_region_page_store.go`
- **Change**: Added per-instance mutex `mu sync.RWMutex` to `regionBackedBTreePageStore`
- **Effect**: Reduces global lock contention for concurrent index operations
- **Status**: ✅ Complete

### Optimization #3: Hot Path Allocations (P0)
- **File**: `dbops.go`
- **Change**: Pre-allocate exact buffer size for record encoding
- **Effect**: Reduced from 704 to ~699 allocations/op
- **Status**: ✅ Complete

### Optimization #5: Field Offset Cache (P1)
- **File**: `indexing.go`
- **Change**: Added `fieldOffsetCache map[string]FieldOffset` for O(1) field lookup
- **Effect**: Eliminated O(n) linear search on every insert
- **Status**: ✅ Complete

### Results After Optimizations
| Metric | Before | After |
|--------|--------|-------|
| Insert | ~140k ns/op | ~127k ns/op |
| QueryInt | ~170k ns/op | ~146k ns/op |
| Peak Heap (inserts) | ~9.5 MB | ~9.5 MB |
| Peak Heap (queries) | ~2.5 MB | ~1.85 MB |

---

## Notes

- All optimizations must maintain backwards compatibility
- Run full test suite (including vacuum) before merging
- Profile with `go test -bench=. -cpuprofile=cpu.out -memprofile=mem.out`
