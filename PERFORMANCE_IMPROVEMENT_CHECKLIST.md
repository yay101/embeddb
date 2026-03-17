# Performance Improvement Checklist (Insert + Query)

Goal: recover/regress-proof insert throughput and improve query latency before further architecture changes.

## Baseline + Measurement Discipline

- [ ] Freeze a repeatable benchmark/test command set for all comparisons
  - [ ] `TestPerfTableConcurrentGoroutines`
  - [ ] `TestPerfTableConcurrent`
  - [ ] `TestPerfTableReopen`
  - [ ] `BenchmarkMemoryInsertWithIndex`
  - [ ] `BenchmarkQueryInt`
- [ ] Capture baseline metrics from current `main` in a dated report file
- [ ] Run each benchmark at least 3x and track median + p95
- [ ] Ensure perf runs are executed serially (no parallel test contamination)

## Insert Path Improvements

- [ ] Add batched region-overlap checks in write path
  - [ ] Check every N writes or when crossing a guard threshold, not every write
  - [ ] Keep hard safety guarantee when overlap risk exists
- [ ] Reduce lock contention in region metadata path
  - [ ] Avoid global catalog lock on common insert fast-path
  - [ ] Lock only on region growth/relocation events
- [ ] Minimize header/catalog writes on insert-heavy workloads
  - [ ] Defer metadata sync to checkpoint boundaries (`Sync`/`Close`) where safe
- [ ] Reduce per-insert allocations in index update path
  - [ ] Reuse buffers in node encode/decode where possible
  - [ ] Reuse temporary slices for key/value operations

## Query Path Improvements

- [ ] Profile point-query path (`QueryInt`) with CPU + alloc profiling
- [ ] Reduce allocations in query read/decode path
  - [ ] Reuse page buffers/read structs where safe
  - [ ] Avoid unnecessary interface conversions in key comparisons
- [ ] Audit lock scope for read operations
  - [ ] Ensure query hot path stays on read locks only
  - [ ] Avoid lock upgrades/re-acquire patterns
- [ ] Validate B-tree node decode hotspots
  - [ ] Optimize decode loops for fixed-width key types

## Region Catalog + Storage Efficiency

- [ ] Cache region entry location/capacity aggressively with versioned invalidation
- [ ] Ensure relocation updates only invalidate impacted caches
- [ ] Add guardrail metrics/logging around relocation frequency
  - [ ] relocation count
  - [ ] bytes moved
  - [ ] average interval between relocations

## Regression + Correctness Gates

- [ ] Re-run full focused correctness suite after each perf change
  - [ ] nested struct query/range tests
  - [ ] slice/time query tests
  - [ ] reopen durability tests
  - [ ] no-runtime-idx test
  - [ ] region corruption recovery tests
- [ ] Block perf merges if any correctness regression appears

## Acceptance Targets (Adjustable)

- [ ] Insert throughput: recover to within 25% of `v0.3.3` baseline
- [ ] `BenchmarkMemoryInsertWithIndex`: reduce by at least 30% from current main
- [ ] `BenchmarkQueryInt`: reduce by at least 20% from current main median
- [ ] No regressions in reopen consistency or nested query behavior

## Output Artifacts

- [ ] Add `perf_reports/` folder with before/after benchmark snapshots
- [ ] Add short summary in README/CHANGELOG once targets are met
