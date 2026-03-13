#!/usr/bin/env sh
set -eu

go test -run '^$' -bench 'Benchmark(Insert|GetByID|QueryString|DeleteNoSecondaryIndexes|DeleteWithThreeSecondaryIndexes|FilterNested|FilterAll|Scan|ScanEarlyExit)$' -benchmem -count=1 ./...
