# embeddb Versioning Fixes & Optimizations

## Critical Bugs (Fixed)

- [x] **Delete doesn't clean up version index** - When a record is deleted, version index entries remain until Vacuum
- [x] **insertLocked missing version support** - InsertMany doesn't track versions
- [x] **Version number calculation assumes sorted** - Race condition in Update

## Performance Optimizations (Completed)

- [x] **Remove redundant GetVersions() calls in Update()** - Now calls GetVersions once instead of 3 times
- [x] **Fix key length limitation in version catalog** - Changed from `byte(len(key))` to `varint` encoding

## Skipped (Not Needed)

- **Optimize versionIndex with map** - O(n) lookup is fine since n is small (bounded by MaxVersions)

## Testing

- [x] Run existing tests - All pass

---

# B-Tree Migration Fixes & Optimizations

## Critical Bugs (Fixed)

- [x] **packedValToSlice/sliceToPackedVal lose secondary index data** - Replaced broken uint64 packing with composite keys (key + recordID suffix), each record gets its own btree entry. (`btree.go:717-744`)
- [x] **Vacuum() downgrades B-tree to in-memory mapIndex** - Now rebuilds a persistent btreeMapIndex after vacuum and writes the root offset to the header. (`main.go:1203`)
- [x] **load() re-scans all records defeating B-tree persistence** - Now skips the record scan when pkIndexBTRoot > 0, trusting the persistent B-tree. Falls back to scanning only for legacy files or new databases. (`main.go:614-648`)
- [x] **Put()/deleteKey() don't handle internal node keys** - Rewrote Put() to use Insert-like logic with full-root splitting. Fixed insertNonFull to update values at internal nodes. Fixed deleteFromNode to replace internal keys with in-order predecessor. Removed broken deleteKey function. (`btree.go:356-396`)
- [x] **Data races in B-tree cache and Count()** - Added separate cacheMu for thread-safe cache access during concurrent reads. Added db.mu.RLock to Count(). (`btree.go, table_api.go`)

## Performance Issues (Fixed)

- [x] **No-op Range call during load** - Removed useless full B-tree traversal. (`main.go:597-599`)
- [x] **Size() does full tree scan - O(n) instead of O(1)** - Added count field to BTree, maintained on insert/delete. (`btree.go:584,700`)
- [x] **SortedKeys() redundantly sorts already-sorted B-tree output** - Removed sort; uses dedup-only since btree scan is in-order. (`btree.go:697`)
- [x] **Linear search in B-tree nodes instead of binary search** - Replaced all linear scans with sort.Search (binary search). ~14x fewer comparisons per node. (`btree.go:267,285,etc`)
- [x] **Double write in insertNonFull for non-leaf case** - Removed redundant writeNode call after recursion. (`btree.go:301`)
- [x] **LRU cache is inefficient with duplicate entries and O(n) eviction** - Replaced slice-based LRU with ring buffer + dedup via map check. O(1) eviction. (`btree.go:232-240`)
- [x] **Excessive allocations in writeNode/readNode** - Added sync.Pool for page buffers in writeNode. Single-allocation key buffer in readNode. (`btree.go:137,208`)

## Testing

- [x] All tests pass
- [x] Race detector clean (`go test -race`)
