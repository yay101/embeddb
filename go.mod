module github.com/yay101/embeddb

go 1.25.8

require (
	github.com/yay101/embeddbcore v0.3.0
	github.com/yay101/embeddbmmap v0.1.0
)

require golang.org/x/sys v0.43.0 // indirect

// v1.8.0 - Migrate to embeddbmmap (Linux mremap), fix flush data loss, fix B-tree readNode
