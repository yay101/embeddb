module github.com/yay101/embeddb

go 1.25.8

require (
	github.com/edsrzf/mmap-go v1.2.0
	github.com/yay101/embeddbcore v0.3.0
)

require golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect

// v1.7.5 - Fix Scanner type mismatch for int PK
// - Fix Scanner decoding int PK as int64 instead of int
// - Fix Scanner not stopping on error
