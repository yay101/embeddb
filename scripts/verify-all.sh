#!/usr/bin/env sh
set -eu

"$(dirname "$0")/test-critical.sh"
"$(dirname "$0")/bench-core.sh"
