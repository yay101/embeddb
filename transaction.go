package embeddb

import "fmt"

type Transaction struct {
	db                   *database
	indexRootSnapshot    uint64
	committed            bool
	recordCounts         map[string]uint32
	newTxnPages          map[uint64]bool
	recordCountSnapshot  map[string]uint32
	nextRecordIDSnapshot map[string]uint32
	allocSnapshot        allocatorSnapshot
}

func (db *database) Begin() *Transaction {
	db.mu.Lock()
	defer db.mu.Unlock()

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
		recordCounts:         make(map[string]uint32),
		newTxnPages:          make(map[uint64]bool),
		recordCountSnapshot:  recordCountSnapshot,
		nextRecordIDSnapshot: nextRecordIDSnapshot,
		allocSnapshot:        allocSnapshot,
	}
	db.tx = tx
	return tx
}

func (tx *Transaction) Commit() error {
	if tx.committed {
		return fmt.Errorf("transaction already committed")
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

func (tx *Transaction) Rollback() error {
	if tx.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}

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
