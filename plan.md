# EmbedDB — Investigation & Path Forward

## CRC Error Investigation

### Symptom
```
btree page 148600737: CRC mismatch (stored=62616261 computed=6b403117)
[type=unknown hex=000101015c440000 RECORD_DATA?]
```
- `stored=62616261` = ASCII `"baab"` (`strings.Repeat("ab", 5000)` from torture test notes pool)
- File CRC also mismatches → data synced to disk, not mmap coherence
- Same page offset gets multiple errors, stored CRC varies between reads
- Reproduces at 3+ concurrent workers, ~12 errors per 312K operations

### Root Cause: In-place B-tree mutations incompatible with rollback

The B-tree mutates nodes **in-place** — `splitChild`, `insertNonFull`, `deleteFromNode`,
`borrowFromLeft`, `borrowFromRight`, `mergeNodes` all call `writeNode` which writes
back to the **same disk offset**. Only `newNode` allocates a fresh page.

**Mechanism:**
1. Transaction begins: snapshots root offset (R) + allocator state (A)
2. Insert triggers split: internal node P at offset 1000 modified in-place — gets new
   Children[i] = 2000 (allocated during transaction)
3. Rollback: root restored to R, allocator restored to A (freeing offset 2000)
4. Root R still references P at offset 1000 — **but P was overwritten in step 2**
5. P still has Children[i] = 2000 from during the transaction
6. Offset 2000 gets re-allocated for records → record data overwrites B-tree page
7. Subsequent traversal follows Children[i] to 2000 → reads record data → CRC mismatch

The corruption is **persistent** because `writeNode` writes through to both mmap and
file. Once a page is overwritten, the original data is gone. The allocator restore
only affects allocation state, not on-disk page content.

### Affected Code Paths

| Function | In-Place Writes | During Transaction? | Risk |
|---|---|---|---|
| `insertNonFull` (leaf) | Writes leaf to existing offset | Yes (if within tx) | Medium |
| `insertNonFull` (internal) | No direct write (recurses) | N/A | Low |
| `splitChild` | Writes child, parent to existing offset; newNode to fresh | Yes | **High** |
| `deleteFromNode` | Writes leaf/internal to existing offset | Yes | Medium |
| `borrowFromLeft/Right` | Writes left/right/parent in-place | Yes | Medium |
| `mergeNodes` | Writes left/parent in-place | Yes | Medium |
| `BulkInsert` | All writes to fresh pages (via `buildInternalNodes`) | No in-place writes | Low |

The **high-risk** path is `splitChild`: it adds new Children entries referencing
transaction-allocated pages. After rollback, those children are freed but the parent
still points to them.

---

## All Issues Found

### Critical (causes data corruption)
1. **In-place mutations break rollback** — See Root Cause above. Any `writeNode` call
   inside a transaction overwrites pre-transaction page data.

### High (causes data loss or incorrect behavior)
2. **`BulkInsert` replaces entire tree** — Builds a new tree from only the provided
   entries, orphaning ALL existing keys from other tables. Multi-table databases
   lose data after any BulkInsert.
3. **`InsertManyBulk` bypasses transaction tracking** — Directly modifies
   `entry.RecordCount` without checking `t.db.tx`. On rollback, record counts
   are restored from snapshot but NextRecordID may be inconsistent.
4. **`bt.count = 0` unprotected in Rollback** — Runs after `bt.mu.Unlock()` in
   `SetRootOffset`. Between these, a concurrent reader sees stale count. **(Fixed)**

### Medium (bugs with workarounds)
5. **B-tree pages never freed** — No `Free()` calls in production code. Orphaned
   pages from splits/merges/deletes/BulkInserts accumulate. File grows unbounded.
   Vacuum can reclaim some but can't detect all orphans.
6. **Missing `children[i]` bounds checks** — 7 call sites accessed `Children[i]`
   without `len(Children)` guard. Would crash or return bad data if page was
   corrupted. **(Fixed)**

### Low (code quality)
7. **`newTxnPages` allocated but never used** — `transaction.go:14` declares the map
   but nothing reads or writes to it.
8. **`Transaction` type not integrated** — Struct exists but transaction semantics
   (isolation, atomic visibility) are incomplete. Rollback clears cache and restores
   allocator but can't restore overwritten page data.

---

## Path Forward Options

### Option A: Full CoW B-tree (Recommended)

Rewrite B-tree mutations to be fully Copy-on-Write. Every modified node gets a new
page. Parent nodes propagate new child offsets upward. Root offset changes on every
mutation.

**What was tried:**
- **Cascading CoW** — `cloneForWrite` at each level, write new page, update parent
  reference. Correct but O(depth) allocations per insert. Caused OOM/SIGBUS in
  long-running tests due to page explosion (~GB of pages over 15s of inserts).
- **Redirect map** — `cowMap[oldOffset] = newOffset`, `readNode` follows redirects.
  Avoids cascading but map grows unboundedly (never cleaned up after commit).

**What's needed:**
A disciplined shadow-paging design where:
1. `writeNode` always allocates a new page when a transaction is active
2. Parent references are updated via a `redirectMap` indexed by original offset
3. On commit: bake in redirects by walking reachable nodes and rewriting
   Children[i] in-place (now that old pages are garbage), then clear the map
4. On rollback: clear redirect map, allocator restore handles everything

**Scope:** ~300-500 lines in `btree.go`. Touches `writeNode`, `insertNonFull`,
`splitChild`, `deleteFromNode`, `rebalance`, `borrowFromLeft/Right`, `mergeNodes`.

**Risk:** Medium. All mutation paths must be converted. Missed paths corrupt silently.

### Option B: Shadow Page Isolated Subtree

Instead of CoW for every node, only shadow pages on the path from root to modified
leaf. Internal nodes below the root are written to new pages; the root itself is
updated. This is what Option A does but with stricter scope.

**Difference from A:** Same approach, just a recognition that only nodes on the
insertion/delete path need shadow copies.

### Option C: Pre-transaction page snapshot + restore

Before the first write in a transaction, snapshot ALL pages that will potentially
be modified. On rollback, restore the snapshots byte-for-byte. Reads during
transaction read from the modified (in-place) pages.

**Approach:**
1. On `Begin`: record all page offsets reachable from root (full tree walk)
2. Before `writeNode` overwrites a page: copy page data to a snapshot buffer
3. On rollback: write snapshot buffers back to their original offsets
4. On commit: discard snapshot buffers

**Pros:** All existing mutation code works unchanged. No structural changes.
**Cons:** Full tree walk on Begin (expensive for large trees). Snapshot storage
overhead (~1 page per modified page). Rollback must write-back before allocator
restore.

**Scope:** ~100 lines. Hook into `writeNode` to snapshot before write, hook into
`rollback()` to restore. Tree walk on `begin()`.

### Option D: Transaction-isolated allocator region

Allocate pages for the transaction in a dedicated region. Parent pages within the
region reference each other; on rollback, the entire region is discarded. The root
switches atomically on commit.

**Approach:**
1. On `Begin`: record current root + allocator state
2. All `writeNode` calls during transaction: allocate from a transient arena
3. When root offset changes (due to CoW or split), record new root
4. On commit: final root becomes permanent, arena pages are committed
5. On rollback: arena pages freed, root restored

**Pros:** Clean separation of transaction data. No in-place overwrites of
pre-transaction pages. **Cons:** Requires separate allocator for transactions
or careful tracking of which allocations belong to the transaction. Root-swap
logic needed for all mutation paths.

### Option E: Remove transactions, fix BulkInsert instead

Accept that transactions are not viable without major B-tree rewrite. Focus on
fixing the BulkInsert tree-replacement bug and orphaned page accumulation.

**What changes:**
1. Fix BulkInsert to merge with existing tree (or document limitation)
2. Keep `begin/commit/rollback` as no-ops with a logged warning
3. Focus on single-writer correctness and performance
4. Remove `Transaction` struct and dead `newTxnPages` map

**Pros:** No CoW overhead. All existing code stays as-is. B-tree operations are
simpler and faster. **Cons:** No isolation or rollback. Applications must
implement their own undo logic.

**Current status:** This option is effectively the current state after this
session — transaction API is hidden (lowercase), transaction tests are skipped,
README documents unavailability.

---

## Changes Made This Session

### A. Defensive validation
- `readNode` bounds validator: validates `len(Children) == Count+1` and all
  child offsets `>= FileHeaderSize`
- Bounds checks at 7 vulnerable call sites (`insertNonFull`, `deleteFromNode`,
  `rebalance`, `findMax`, `Scan`)

### B. Race fix
- `bt.count = 0` moved inside `SetRootOffset` under `bt.mu.Lock()`
- Removed unprotected `tx.db.index.count = 0` from `Rollback`

### C. API hiding
- `DB.Begin/Commit/Rollback` → lowercase unexported `begin/commit/rollback`
- `Transaction` → `transaction` (unexported)
- All 8 transaction tests skipped with `t.Skip("transactions are an internal/immature feature")`
- `torture/main.go`: removed `runTransaction`, widened adjacent op ranges

### D. Documentation
- README Transactions section: replaced example code with explanation of why
  unavailable + root cause + path to re-enable
- Features list: strikethrough on transactions entry
- Known Limitations: added transaction unavailability note
- `plan.md`: this file, with full investigation and path-forward options

---

## Recommendation

**Short-term (now):** Option E is already implemented — transactions are hidden
and documented as unavailable. Focus on single-writer correctness.

**Medium-term:** Option C (page snapshot + restore) is the lowest-risk path to
re-enabling transactions. A tree walk on Begin + page copy before writeNode is
straightforward and doesn't require touching any B-tree mutation logic. The
snapshot storage cost is 4KB per modified page, bounded by tree depth per insert.

**Long-term:** Option A (full CoW) is the correct architectural fix. It requires
rewriting all B-tree mutation paths but eliminates the in-place modification
problem entirely and enables true MVCC. The redirect-map approach with commit-time
bake-in is the most promising variant tested so far.
