package embeddb

import "fmt"

// Transaction represents a copy-on-write transaction. All writes within a transaction
// are isolated until Commit is called. Rollback restores the database to its pre-transaction state.
// Only one transaction can be active at a time per database.
type Transaction struct {
	db                   *database
	indexRootSnapshot    uint64
	committed            bool
	rolledBack           bool
	recordCounts         map[string]uint32
	newTxnPages          map[uint64]bool
	recordCountSnapshot  map[string]uint32
	nextRecordIDSnapshot map[string]uint32
	allocSnapshot        allocatorSnapshot
}

func (db *database) Begin() *Transaction {
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

	tx := &Transaction{
		db:                   db,
		indexRootSnapshot:    indexRootSnapshot,
		committed:            false,
		rolledBack:           false,
		recordCounts:         make(map[string]uint32),
		newTxnPages:          make(map[uint64]bool),
		recordCountSnapshot:  recordCountSnapshot,
		nextRecordIDSnapshot: nextRecordIDSnapshot,
		allocSnapshot:        allocSnapshot,
	}
	db.tx = tx
	return tx
}

// Commit applies the transaction's changes to the database. After Commit, the transaction
// cannot be used again.
func (tx *Transaction) Commit() error {
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
func (tx *Transaction) Rollback() error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}
	if tx.rolledBack {
		return fmt.Errorf("transaction already rolled back")
	}
	tx.rolledBack = true

	tx.db.index.SetRootOffset(tx.indexRootSnapshot)
	tx.db.index.count = 0
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
