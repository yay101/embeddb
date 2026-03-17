# Secondary Index Region Migration Checklist

Goal: remove runtime `.idx` files by moving secondary indexes to a page-based region inside the main DB file, while preserving all current query semantics and performance characteristics.

## Non-Negotiable Feature Contract

- [ ] Keep nested struct indexing/query behavior (including dot-path field lookups)
- [ ] Keep scalar/time/slice index query behavior
- [ ] Keep range query semantics and inclusivity flags
- [ ] Keep paged query semantics (`offset`, `limit`, `hasMore`, `totalCount`)
- [ ] Keep multi-type same-file behavior across concurrent handles
- [ ] Keep reopen durability for primary and secondary indexes

## Phase 1: Refactor Safety Net

- [ ] Add regression-focused coverage labels for:
  - nested struct queries
  - time queries/ranges
  - slice indexing
  - reopen consistency
  - multi-handle consistency
- [ ] Add a concise "behavior invariants" developer note in code comments near index manager and B-tree entry points

## Phase 2: Storage Abstraction (No Behavior Change)

- [ ] Introduce B-tree page-store abstraction (`ReadPage`, `WritePage`, `AllocPage`, `FreePage`, `Flush`, `Close`)
- [ ] Implement file-backed page store using current `.idx` behavior
- [ ] Route B-tree core IO calls through abstraction without changing algorithms
- [ ] Ensure full existing tests still pass at this stage

## Phase 3: DB-Region Page Store

- [ ] Extend DB header with secondary-index region metadata:
  - region start
  - region capacity
  - region used
  - metadata/catalog offset
  - format version
- [ ] Implement region superblock + validation (magic/version)
- [ ] Implement fixed-page region IO (4KB pages)
- [ ] Implement growth policy with reserve buffer (grow only when needed)
- [ ] Implement relocation logic only when region capacity is exceeded

## Phase 4: Region Catalog + Index Lifecycle

- [ ] Implement catalog entries for table/field index metadata
- [ ] Wire `CreateIndex`/`DropIndex`/`CheckIndexes` to region catalog
- [ ] Remove runtime dependence on filesystem index discovery
- [ ] Keep incremental writes and write-behind caching behavior

## Phase 5: Migration and Compatibility Bridge

- [ ] One-time import from legacy `.idx` files into region
- [ ] One-time import from legacy footer blob format (if present)
- [ ] Make migration idempotent and crash-safe
- [ ] Remove legacy artifacts only after successful import

## Phase 6: Durability and Recovery Hardening

- [ ] Define write ordering (`pages -> catalog -> header`)
- [ ] Add generation/checkpoint marker for startup recovery
- [ ] Add corruption handling path (rebuild affected index only)

## Phase 7: Performance Validation

- [ ] Compare query latency/throughput before vs after for:
  - point query
  - range query
  - nested query
  - mixed read/write workload
- [ ] Verify no regressions in long-running perf/vacuum tests
- [ ] Tune growth buffer and allocation chunk sizes if needed

## Phase 8: Beta Cleanup

- [ ] Remove obsolete file-merge/extract helpers
- [ ] Keep stats/introspection API for embedded secondary-index region
- [ ] Update README/docs for beta storage architecture
- [ ] Publish migration notes and operational caveats

## Done Criteria

- [ ] No runtime `.idx` files in normal operation
- [ ] All required query features match existing behavior
- [ ] Reopen durability is verified
- [ ] Long-running tests pass with expected durations
- [ ] Migration path is stable and documented
