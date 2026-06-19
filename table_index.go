package embeddb

import (
	"bytes"

	"github.com/yay101/embeddbcore"
)

func (t *Table[T]) insertSecondaryKeys(record *T, recordID uint32, offset uint64) {
	if t.db.parent == nil {
		return
	}
	if !t.db.parent.autoIndex {
		if t.db.explicitIndexes == nil {
			return
		}
		explicitFields := t.db.explicitIndexes[t.name]
		if len(explicitFields) == 0 {
			return
		}
		explicitSet := make(map[string]bool, len(explicitFields))
		for _, f := range explicitFields {
			explicitSet[f] = true
		}
		for _, field := range t.layout.Fields {
			if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice && explicitSet[field.Name] {
				if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
					continue
				}
				key := embeddbcore.GetFieldAsString(record, field)
				if key != "" {
					secKey := encodeSecondaryKey(t.tableID, field.Name, key, recordID)
					t.db.index.Insert(secKey, offset)
				}
			}
		}
		return
	}
	for _, field := range t.layout.Fields {
		if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice {
			if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
				continue
			}
			key := embeddbcore.GetFieldAsString(record, field)
			if key != "" {
				secKey := encodeSecondaryKey(t.tableID, field.Name, key, recordID)
				t.db.index.Insert(secKey, offset)
			}
		}
	}
}

func (t *Table[T]) deleteSecondaryKeys(record *T, recordID uint32, offset uint64) {
	if t.db.parent == nil {
		return
	}
	if !t.db.parent.autoIndex {
		if t.db.explicitIndexes == nil {
			return
		}
		explicitFields := t.db.explicitIndexes[t.name]
		if len(explicitFields) == 0 {
			return
		}
		explicitSet := make(map[string]bool, len(explicitFields))
		for _, f := range explicitFields {
			explicitSet[f] = true
		}
		for _, field := range t.layout.Fields {
			if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice && explicitSet[field.Name] {
				if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
					continue
				}
				key := embeddbcore.GetFieldAsString(record, field)
				if key != "" {
					secKey := encodeSecondaryKey(t.tableID, field.Name, key, recordID)
					t.db.index.Delete(secKey)
				}
			}
		}
		return
	}
	for _, field := range t.layout.Fields {
		if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice {
			if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
				continue
			}
			key := embeddbcore.GetFieldAsString(record, field)
			if key != "" {
				secKey := encodeSecondaryKey(t.tableID, field.Name, key, recordID)
				t.db.index.Delete(secKey)
			}
		}
	}
}

// CreateIndex builds a secondary index for the specified field by scanning all existing
// records. If auto-indexing is disabled, the field is added to the explicit index list.
// Returns nil if the index already exists.
func (t *Table[T]) CreateIndex(fieldName string) error {
	if t.db.parent != nil && !t.db.parent.autoIndex {
		t.db.mu.Lock()
		if t.db.explicitIndexes == nil {
			t.db.explicitIndexes = make(map[string][]string)
		}
		found := false
		for _, f := range t.db.explicitIndexes[t.name] {
			if f == fieldName {
				found = true
				break
			}
		}
		if !found {
			t.db.explicitIndexes[t.name] = append(t.db.explicitIndexes[t.name], fieldName)
		}
		t.db.mu.Unlock()
	}

	field, err := t.findField(fieldName)
	if err != nil {
		return err
	}

	prefix := encodeSecondaryKeyPrefix(t.tableID, fieldName)
	var existing int
	t.db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			existing++
		}
		return true
	})
	if existing > 0 {
		return nil
	}

	scanner := t.ScanRecords()
	defer scanner.Close()
	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		key := embeddbcore.GetFieldAsString(record, field)
		if key != "" {
			pkVal, _ := t.getPKValue(record)
			pkBytes := encodePrimaryKey(t.tableID, t.normalizePK(pkVal))
			offset, err := t.db.index.Get(pkBytes)
			if err != nil {
				continue
			}
			recordID, err := t.getRecordIDAt(offset)
			if err != nil {
				continue
			}
			secKey := encodeSecondaryKey(t.tableID, fieldName, key, recordID)
			t.db.index.Insert(secKey, offset)
		}
	}

	if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil {
		delete(t.db.droppedIndexes[t.name], fieldName)
	}

	return nil
}

// DropIndex removes all secondary index entries for the specified field and marks
// the field as dropped to prevent auto-reindexing.
func (t *Table[T]) DropIndex(fieldName string) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	prefix := encodeSecondaryKeyPrefix(t.tableID, fieldName)
	var keys [][]byte
	t.db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			keys = append(keys, keyCopy)
		}
		return true
	})
	for _, k := range keys {
		t.db.index.Delete(k)
	}

	if t.db.droppedIndexes == nil {
		t.db.droppedIndexes = make(map[string]map[string]bool)
	}
	if t.db.droppedIndexes[t.name] == nil {
		t.db.droppedIndexes[t.name] = make(map[string]bool)
	}
	t.db.droppedIndexes[t.name][fieldName] = true

	return nil
}

// InsertRawIndex inserts an arbitrary key-offset pair into the table's B-tree index.
// This allows external callers to manage secondary indexes for fields not present
// on the struct type T. Used by network layers (e.g. netembeddb).
func (t *Table[T]) InsertRawIndex(key []byte, offset uint64) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	return t.db.index.Insert(key, offset)
}

// DeleteRawIndex removes an arbitrary key from the table's B-tree index.
func (t *Table[T]) DeleteRawIndex(key []byte) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	return t.db.index.Delete(key)
}

// ScanRawIndex iterates over index keys in the range [start, end) using the B-tree.
// If fn returns false, iteration stops early.
// This enables external callers to perform index-based queries.
func (t *Table[T]) ScanRawIndex(start, end []byte, fn func(key []byte, value uint64) bool) error {
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()
	return t.db.index.ScanRange(start, end, fn)
}

// GetRawIndex returns the offset for a given index key. Returns an error
// if the key is not found.
func (t *Table[T]) GetRawIndex(key []byte) (uint64, error) {
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()
	return t.db.index.Get(key)
}

// GetIndexedFields returns the list of fields that currently have secondary indexes.
func (t *Table[T]) GetIndexedFields() []string {
	var fields []string
	if t.db.parent != nil && t.db.parent.autoIndex {
		for _, field := range t.layout.Fields {
			if field.Name != "" && !field.Primary && field.Offset > 0 && !field.IsSlice {
				if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][field.Name] {
					continue
				}
				fields = append(fields, field.Name)
			}
		}
	} else {
		if t.db.explicitIndexes != nil {
			for _, f := range t.db.explicitIndexes[t.name] {
				if t.db.droppedIndexes == nil || t.db.droppedIndexes[t.name] == nil || !t.db.droppedIndexes[t.name][f] {
					fields = append(fields, f)
				}
			}
		}
	}
	return fields
}
