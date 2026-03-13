#!/usr/bin/env sh
set -eu

go test -v -run 'Test(TableDrop|MockMultiTableVacuumReopenNested100K|MultiTypeSameFileConcurrentHandles|NestedStructQuery|NestedStructRangeQuery|OpenUseMultiType|PerfTableReopen|TimeMmapReload)$' ./...
