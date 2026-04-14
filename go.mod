module github.com/yay101/embeddb

go 1.25.8

require (
	github.com/edsrzf/mmap-go v1.2.0
	github.com/yay101/embeddbcore v0.3.0
)

require golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect

// v1.7.2 - Fix secondary index updates on Update/Delete/Upsert
// - Fix Update not removing old index entries
// - Fix Delete/DeleteMany not cleaning up indexes  
// - Fix updateLocked reading recordID from wrong location
// - Add DeleteFromIndexes method to indexManager
// - Add tests for large structs with upserts
