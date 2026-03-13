#!/usr/bin/env sh
set -eu

go test -v -run 'TestPerfTable(Operations|100K|Concurrent)$' ./...
