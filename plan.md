# EmbedDB — Codebase Audit & Fix Plan

## Phase 1: Critical (crash / data corruption)

| # | Issue | File(s) | Fix |
|---|-------|---------|-----|
| 1 | WAL broken after replay — `Replay` closes WAL file, `openDatabase` never reopens → nil panic on next write | `wal.go:157`, `database.go:316` | Reopen WAL after replay |
| 2 | B-tree writes bypass WAL — `writeNode` writes directly to mmap, index not recoverable after crash | `btree.go:346` | Document limitation or log page writes to WAL |
| 3 | Region reload race — `pageData` reloads region after `RLock` on old one → SIGBUS | `btree.go:174,346` | Pass loaded region into `pageData` |
| 4 | `resizeCache` out-of-bounds — `cacheHead` can equal `newSize` → panic | `btree.go:607` | `cacheHead = n % newSize` |
| 5 | Rollback leaves stale on-disk root — crash after rollback reopens with transaction's root | `transaction.go:87` | Write restored root to file header immediately |
| 6 | `deleteSecondaryKeys` no-op without autoIndex — explicit indexes never cleaned | `table_index.go:53` | Mirror `insertSecondaryKeys` explicit-index logic |
| 7 | `deactivateRecord` swallows all errors — failed reads write zeros, clobbering records | `table_api.go:747` | Return and propagate errors |

## Phase 2: High (data loss / incorrect behavior)

| # | Issue | File(s) |
|---|-------|---------|
| 8 | `updateLocked` overwrites version 1, skips maxVersions pruning | `table_crud.go:692` |
| 9 | `Vacuum` error paths leave DB with no file, no recovery | `vacuum.go:126` |
| 10 | `Vacuum` ignores read errors during copy → garbage output | `vacuum.go:69,90` |
| 11 | 7 B-tree operations discard `writeNode` errors | `btree.go:955+` |
| 12 | `Update`/`updateLocked` ignore PK Insert error | `table_crud.go:307` |
| 13 | Map fields tagged `encrypt` stored in plaintext | `record.go:626` |
| 14 | `Backup` not crash-consistent — no mmap sync, no db.mu | `backup.go:46` |
| 15 | `Sync`/`Vacuum`/`FastSync` call flush N times (once per table) | `table_api.go:540` |
| 16 | `encodeSecondaryKeyEndPrefix` wraps at tableID 255 | `index.go:107` |

## Phase 3: Performance

| # | Issue | Impact |
|---|-------|--------|
| 18 | `InsertManyBulk` double-encodes every record | 2× encoding |
| 23 | Range queries decode via string round-trip | String alloc per key |
| 24 | `ListVersions`/`Update` O(n) scan instead of ScanRange | Linear version lookup |
| 25 | `ScanRecords` loads all PKs into memory upfront | O(n) allocation |
| 32 | `allocator.Free` O(n) insert + O(n) coalesce | Hotspot under deletes |
| 33 | `Drop` scans entire index O(n) | Slow table drop |
| 47 | `encodeSliceOfStructs` recomputes layout every call | Layout recompute per slice |

## Phase 4: Hardening & code quality

| # | Issue |
|---|-------|
| 17 | Migration leaves stale index entries for skipped records |
| 20 | `DeleteMany` empty `if err != nil {}` body |
| 21 | `alloc.nextOffset` accessed without `alloc.mu` |
| 22 | `matchLike` doesn't handle `_` wildcard |
| 26 | `Scan`/`Count` access `tableCat` without `db.mu` |
| 27 | `Verify` nests `RWMutex.RLock` → deadlock |
| 28 | `allocator.Load` doesn't bound `blockCount` → OOM |
| 29 | Encode/decode silently skip on error — fields vanish |
| 30 | `recordTotalSize` can OOM on corrupt `PayloadLen` |
| 31 | WAL `writeEntry` buffer not concurrency-safe |
| 34 | `Use` on existing table doesn't detect schema change |
| 36 | 8 pieces of dead code |
| 37 | `openDatabase` migrate parameter unused |
| 42 | `hdrBufPool` returns `[]byte` not `*[]byte` |
| 54 | `Decode*Slice` allocate from untrusted length |
| 55 | `go.sum` missing entry |
| 57 | `Decode*Slice` return nil error on truncation |
| 58 | Scan errors ignored in 5+ places |
| 59 | `runTransaction` only implements case 0 of 3 |

## Phase 5: Testing gaps

| # | Missing test |
|---|-------------|
| 60 | WAL replay + subsequent writes |
| 61 | Rollback + crash/reopen |
| 62 | resizeCache at exact capacity |
| 63 | Explicit-index Delete/Update |
| 64 | Vacuum failure recovery |
| 66 | Fuzz tests for decode/Load/replay |
| 67 | Sync vs Insert WAL concurrency |
