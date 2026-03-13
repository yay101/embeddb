GO ?= go

.PHONY: test-critical-fast test-critical bench-core perf-extended verify-all

TEST_CRITICAL_FAST_REGEX := Test(TableDrop|MockMultiTableVacuumReopenNested100K|MultiTypeSameFileConcurrentHandles|NestedStructQuery|NestedStructRangeQuery|OpenUseMultiType|PerfTableReopen|TimeMmapReload)$$
TEST_VACUUM_LONG_REGEX := TestVacuumFileStats300K$$
BENCH_CORE_REGEX := Benchmark(Insert|GetByID|QueryString|DeleteNoSecondaryIndexes|DeleteWithThreeSecondaryIndexes|FilterNested|FilterAll|Scan|ScanEarlyExit)$$
PERF_EXTENDED_REGEX := TestPerfTable(Operations|100K|Concurrent)$$

test-critical-fast:
	$(GO) test -v -run '$(TEST_CRITICAL_FAST_REGEX)' ./...

test-critical:
	$(GO) test -v -run '$(TEST_CRITICAL_FAST_REGEX)' ./...
	$(GO) test -v -run '$(TEST_VACUUM_LONG_REGEX)' ./...

bench-core:
	$(GO) test -run '^$$' -bench '$(BENCH_CORE_REGEX)' -benchmem -count=1 ./...

perf-extended:
	$(GO) test -v -run '$(PERF_EXTENDED_REGEX)' ./...

verify-all:
	$(MAKE) test-critical
	$(MAKE) bench-core
