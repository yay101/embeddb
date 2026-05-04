module github.com/yay101/embeddb

go 1.25.8

require (
	github.com/yay101/embeddbcore v0.5.1
	github.com/yay101/embeddbmmap v0.1.2
)

require (
	github.com/golang/snappy v1.0.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

// v1.8.0 - Migrate to embeddbmmap (fixed: no MREMAP_MAYMOVE), fix flush data loss, fix B-tree readNode)
