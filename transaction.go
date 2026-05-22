package embeddb

import "fmt"

type transaction struct {
	db                   *database
	indexRootSnapshot    uint64
	committed            bool
	rolledBack           bool
	recordCounts         map[string]uint32
	recordCountSnapshot  map[string]uint32
	nextRecordIDSnapshot map[string]uint32
	allocSnapshot        allocatorSnapshot
	pageSnapshots        map[uint64][]byte
}

func (db *database) begin() *transaction {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.tx != nil && !db.tx.committed && !db.tx.rolledBack {
		return nil
	}

	indexRootSnapshot := db.index.RootOffset()

	recordCountSnapshot := make(map[string]uint32)
	nextRecordIDSnapshot := make(map[string]uint32)
	for name, entry := range db.tableCat {
		recordCountSnapshot[name] = entry.RecordCount
		nextRecordIDSnapshot[name] = entry.NextRecordID
	}

	allocSnapshot := db.alloc.Snapshot()

	tx := &transaction{
		db:                   db,
		indexRootSnapshot:    indexRootSnapshot,
		committed:            false,
		rolledBack:           false,
		recordCounts:         make(map[string]uint32),
		recordCountSnapshot:  recordCountSnapshot,
		nextRecordIDSnapshot: nextRecordIDSnapshot,
		allocSnapshot:        allocSnapshot,
		pageSnapshots:        make(map[uint64][]byte),
	}
	db.tx = tx
	return tx
}

// Commit applies the transaction's changes to the database. After Commit, the transaction
// cannot be used again.
func (tx *transaction) commit() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if tx.committed {
		return fmt.Errorf("transaction already committed")
	}
	if tx.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}
	tx.committed = true
	for name, delta := range tx.recordCounts {
		if entry, ok := tx.db.tableCat[name]; ok {
			entry.RecordCount += delta
		}
	}
	tx.db.tx = nil
	return nil
}

// Rollback discards all changes made within the transaction and restores the database
// to its state before Begin was called. Cannot be called after Commit.
func (tx *transaction) rollback() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}
	if tx.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}
	tx.rolledBack = true

	for offset, oldPage := range tx.pageSnapshots {
		tx.db.writeAt(oldPage, int64(offset))
	}

	tx.db.index.SetRootOffset(tx.indexRootSnapshot)
	tx.db.index.cacheMu.Lock()
	tx.db.index.cache = make(map[uint64]*BTreeNode, 2048)
	tx.db.index.cacheMu.Unlock()

	tx.db.alloc.Restore(tx.allocSnapshot)

	for name, rc := range tx.recordCountSnapshot {
		if entry, ok := tx.db.tableCat[name]; ok {
			entry.RecordCount = rc
		}
	}
	for name, nrid := range tx.nextRecordIDSnapshot {
		if entry, ok := tx.db.tableCat[name]; ok {
			entry.NextRecordID = nrid
		}
	}

	tx.db.tx = nil
	return nil
}
