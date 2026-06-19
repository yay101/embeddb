package embeddb

import (
	"bytes"
	"fmt"

	"github.com/yay101/embeddbcore"
)

// Insert adds a new record to the table. If the primary key is zero-valued, it is
// auto-assigned the next available ID. Returns the record ID.
// Returns an error if the primary key already exists or the table is not found.
func (t *Table[T]) Insert(record *T) (uint32, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]
	if entry == nil {
		return 0, fmt.Errorf("table not found")
	}

	recordID := entry.NextRecordID
	entry.NextRecordID++

	pkVal, _ := t.getPKValue(record)
	if t.isZeroPK(pkVal) {
		t.setPKValue(record, recordID)
		pkVal = recordID
	} else {
		if _, err := t.db.index.Get(encodePrimaryKey(t.tableID, pkVal)); err == nil {
			return 0, fmt.Errorf("primary key already exists: %v", pkVal)
		}
	}

	recordBuf, err := t.encodeRecord(record, recordID, embeddbcore.FlagsActive, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	offset, _, err := t.db.alloc.Allocate(uint64(len(recordBuf)))
	if err != nil {
		return 0, fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(recordBuf, int64(offset)); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	if err := t.db.index.Insert(encodePrimaryKey(t.tableID, pkVal), offset); err != nil {
		return 0, fmt.Errorf("failed to insert primary key: %w", err)
	}

	if t.maxVersions > 0 {
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), offset)
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	t.insertSecondaryKeys(record, recordID, offset)

	t.db.autoSync()

	return recordID, nil
}

func (t *Table[T]) isZeroPK(val any) bool {
	if val == nil {
		return true
	}
	switch v := val.(type) {
	case int:
		return v == 0
	case int8:
		return v == 0
	case int16:
		return v == 0
	case int32:
		return v == 0
	case int64:
		return v == 0
	case uint:
		return v == 0
	case uint8:
		return v == 0
	case uint16:
		return v == 0
	case uint32:
		return v == 0
	case uint64:
		return v == 0
	case string:
		return v == ""
	}
	return false
}

func (t *Table[T]) setPKValue(record *T, val any) {
	for _, field := range t.layout.Fields {
		if field.Primary {
			embeddbcore.SetFieldValue(record, field, val)
			return
		}
	}
}

func (t *Table[T]) getPKValue(record *T) (any, error) {
	for _, field := range t.layout.Fields {
		if field.Primary {
			val, err := embeddbcore.GetFieldValue(record, field)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}
	return nil, fmt.Errorf("no primary key found")
}

// Get retrieves a record by its primary key. Returns ErrKeyNotFound if the record
// does not exist or has been deleted.
func (t *Table[T]) Get(id any) (*T, error) {
	return t.getLocked(t.normalizePK(id))
}

func (t *Table[T]) getLocked(pkValue any) (*T, error) {
	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, pkValue))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	return t.readRecordAt(offset)
}

// GetVersion retrieves a specific version of a record by its primary key and version number.
// Versioning must be enabled (MaxVersions > 0) for this to work.
// Version 1 is the first version after the current record.
func (t *Table[T]) GetVersion(id any, version uint32) (*T, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	hdrBuf := make([]byte, embeddbcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return nil, fmt.Errorf("failed to read record header: %w", err)
	}
	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return nil, fmt.Errorf("invalid record header: %w", err)
	}

	versionOffset, err := t.db.index.Get(encodeVersionKey(t.tableID, hdr.RecordID, version))
	if err != nil {
		return nil, fmt.Errorf("version %d not found for record", version)
	}

	return t.readRecordAt(versionOffset)
}

// ListVersions returns metadata for all stored versions of a record.
// Versioning must be enabled (MaxVersions > 0) for this to work.
func (t *Table[T]) ListVersions(id any) ([]VersionMetadata, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	hdrBuf := make([]byte, embeddbcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return nil, fmt.Errorf("failed to read record header: %w", err)
	}
	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return nil, fmt.Errorf("invalid record header: %w", err)
	}

	prefix := encodeVersionKeyPrefix(t.tableID, hdr.RecordID)
	var results []VersionMetadata
	t.db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			if _, _, ver, ok := parseVersionKey(key); ok {
				results = append(results, VersionMetadata{Version: ver})
			}
		}
		return true
	})

	return results, nil
}

// Update replaces an existing record with new values. If versioning is enabled,
// the old record is preserved as a previous version. The old record is deactivated
// and a new record is allocated.
func (t *Table[T]) Update(id any, record *T) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return fmt.Errorf("record not found")
	}

	hdrBuf := make([]byte, embeddbcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return fmt.Errorf("failed to read record for update: %w", err)
	}

	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return fmt.Errorf("invalid record at offset %d: %w", offset, err)
	}

	if !hdr.IsActive() {
		return fmt.Errorf("record at offset %d is not active", offset)
	}

	recordID := hdr.RecordID

	totalLen := recordTotalSize(hdr)
	oldRecordBuf := make([]byte, totalLen)
	copy(oldRecordBuf, hdrBuf)
	if err := t.db.readAt(oldRecordBuf[embeddbcore.RecordHeaderSize:], int64(offset)+int64(embeddbcore.RecordHeaderSize)); err != nil {
		return fmt.Errorf("failed to read old record data: %w", err)
	}

	var oldRecord T
	t.decodeRecord(oldRecordBuf, &oldRecord)

	t.deleteSecondaryKeys(&oldRecord, recordID, offset)

	var flags byte = embeddbcore.FlagsActive
	var prevVersionOff uint64
	if t.maxVersions > 0 {
		flags |= embeddbcore.FlagsHasPrevVersion
		prevVersionOff = offset
	}

	newRecordBuf, err := t.encodeRecord(record, recordID, flags, prevVersionOff)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	newOffset, _, err := t.db.alloc.Allocate(uint64(len(newRecordBuf)))
	if err != nil {
		return fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(newRecordBuf, int64(newOffset)); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	if t.maxVersions > 0 {
		prefix := encodeVersionKeyPrefix(t.tableID, recordID)
		type verInfo struct {
			version uint32
			offset  uint64
		}
		var versions []verInfo
		t.db.index.Scan(func(key []byte, value uint64) bool {
			if bytes.HasPrefix(key, prefix) {
				if _, _, ver, ok := parseVersionKey(key); ok {
					versions = append(versions, verInfo{version: ver, offset: value})
				}
			}
			return true
		})

		if len(versions) == 0 {
			t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), offset)
			versions = append(versions, verInfo{version: 1, offset: offset})
		}
		newVersion := uint32(1)
		for _, v := range versions {
			if v.version >= newVersion {
				newVersion = v.version + 1
			}
		}
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, newVersion), newOffset)
		versions = append(versions, verInfo{version: newVersion, offset: newOffset})

		maxTotal := int(t.maxVersions) + 1
		for len(versions) > maxTotal {
			oldestIdx := 0
			for i, v := range versions {
				if v.version < versions[oldestIdx].version {
					oldestIdx = i
				}
			}
			_ = t.deactivateRecord(versions[oldestIdx].offset)
			t.db.index.Delete(encodeVersionKey(t.tableID, recordID, versions[oldestIdx].version))
			versions = append(versions[:oldestIdx], versions[oldestIdx+1:]...)
		}
	} else {
		if err := t.deactivateRecord(offset); err != nil {
			return fmt.Errorf("deactivate old record: %w", err)
		}
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, t.normalizePK(id)), newOffset)

	t.insertSecondaryKeys(record, recordID, newOffset)

	t.db.autoSync()

	return nil
}

// Delete removes a record by its primary key. The record is deactivated (not physically
// removed) and its index entries are deleted. Space is reclaimed on Vacuum.
// If versioning is enabled, all versions are also deleted.
func (t *Table[T]) Delete(id any) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]

	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return fmt.Errorf("record not found")
	}

	var recordID uint32
	hdrBuf := make([]byte, embeddbcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		return fmt.Errorf("failed to read record header: %w", err)
	}
	if hdrBuf[0] == V2RecordVersion {
		hdr, _ := decodeRecordHeader(hdrBuf)
		if hdr.IsActive() {
			recordID = hdr.RecordID
			totalLen := recordTotalSize(hdr)
			oldRecordBuf := make([]byte, totalLen)
			copy(oldRecordBuf, hdrBuf)
			if err := t.db.readAt(oldRecordBuf[embeddbcore.RecordHeaderSize:], int64(offset)+int64(embeddbcore.RecordHeaderSize)); err != nil {
				return fmt.Errorf("failed to read record data: %w", err)
			}

			var oldRecord T
			t.decodeRecord(oldRecordBuf, &oldRecord)

			t.deleteSecondaryKeys(&oldRecord, recordID, offset)
		}
	}

	if err := t.deactivateRecord(offset); err != nil {
		return fmt.Errorf("deactivate record %d: %w", id, err)
	}

	t.db.index.Delete(encodePrimaryKey(t.tableID, t.normalizePK(id)))

	if t.maxVersions > 0 && recordID > 0 {
		prefix := encodeVersionKeyPrefix(t.tableID, recordID)
		var versionKeys [][]byte
		t.db.index.Scan(func(key []byte, value uint64) bool {
			if bytes.HasPrefix(key, prefix) {
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				versionKeys = append(versionKeys, keyCopy)
			}
			return true
		})
		for _, k := range versionKeys {
			t.db.index.Delete(k)
		}
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]--
	} else if entry.RecordCount > 0 {
		entry.RecordCount--
	}

	return nil
}

// DeleteMany removes multiple records by their primary keys. Returns the number of
// records successfully deleted. Non-existent keys are silently skipped.
func (t *Table[T]) DeleteMany(ids []any) (int, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]

	deleted := 0
	for _, id := range ids {
		offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
		if err != nil {
			continue
		}

		hdrBuf := make([]byte, embeddbcore.RecordHeaderSize)
		if err := t.db.readAt(hdrBuf, int64(offset)); err != nil {
		}
		if hdrBuf[0] == V2RecordVersion {
			hdr, _ := decodeRecordHeader(hdrBuf)
			if hdr.IsActive() {
				totalLen := recordTotalSize(hdr)
				oldRecordBuf := make([]byte, totalLen)
				copy(oldRecordBuf, hdrBuf)
				if err := t.db.readAt(oldRecordBuf[embeddbcore.RecordHeaderSize:], int64(offset)+int64(embeddbcore.RecordHeaderSize)); err != nil {
					continue
				}

				var oldRecord T
				t.decodeRecord(oldRecordBuf, &oldRecord)

				t.deleteSecondaryKeys(&oldRecord, hdr.RecordID, offset)
			}
		}

		_ = t.deactivateRecord(offset)
		t.db.index.Delete(encodePrimaryKey(t.tableID, t.normalizePK(id)))
		deleted++
		if t.db.tx != nil {
			t.db.tx.recordCounts[t.name]--
		} else if entry.RecordCount > 0 {
			entry.RecordCount--
		}
	}
	return deleted, nil
}

// InsertMany inserts multiple records sequentially. Returns the IDs of successfully
// inserted records and the first error encountered (if any).
func (t *Table[T]) InsertMany(records []*T) ([]uint32, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	var firstErr error
	ids := make([]uint32, 0, len(records))
	for _, record := range records {
		id, err := t.insertLocked(record)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ids = append(ids, id)
	}

	t.db.autoSync()

	return ids, firstErr
}

// InsertManyBulk inserts multiple records using a bulk B-tree build for better performance.
// Records are encoded, allocated contiguously, and inserted into the B-tree in a single
// bulk operation. This is significantly faster than InsertMany for large batches.
func (t *Table[T]) InsertManyBulk(records []*T) ([]uint32, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	entry := t.db.tableCat[t.name]
	if entry == nil {
		return nil, fmt.Errorf("table not found")
	}

	type bulkEntry struct {
		record   *T
		recordID uint32
		offset   uint64
		pkVal    any
		pkKey    []byte
	}

	bulk := make([]bulkEntry, 0, len(records))
	var totalSize uint64

	for _, record := range records {
		recordID := entry.NextRecordID
		entry.NextRecordID++

		pkVal, _ := t.getPKValue(record)
		if t.isZeroPK(pkVal) {
			t.setPKValue(record, recordID)
			pkVal = recordID
		}

		recordBuf, err := t.encodeRecord(record, recordID, embeddbcore.FlagsActive, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to encode record: %w", err)
		}

		bulk = append(bulk, bulkEntry{
			record:   record,
			recordID: recordID,
			pkVal:    pkVal,
			pkKey:    encodePrimaryKey(t.tableID, pkVal),
		})
		totalSize += uint64(len(recordBuf))
	}

	baseOffset, _, err := t.db.alloc.Allocate(totalSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate space: %w", err)
	}

	offset := baseOffset
	var encodedRecords [][]byte
	for i := range bulk {
		recordBuf, err := t.encodeRecord(bulk[i].record, bulk[i].recordID, embeddbcore.FlagsActive, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to encode record: %w", err)
		}
		encodedRecords = append(encodedRecords, recordBuf)
		bulk[i].offset = offset
		if err := t.db.writeAt(recordBuf, int64(offset)); err != nil {
			return nil, fmt.Errorf("failed to write record: %w", err)
		}
		offset += uint64(len(recordBuf))
	}

	btEntries := make([]struct{ key []byte; value uint64 }, len(bulk))
	for i, b := range bulk {
		btEntries[i] = struct{ key []byte; value uint64 }{key: b.pkKey, value: b.offset}
	}

	if err := t.db.index.BulkInsert(btEntries); err != nil {
		return nil, fmt.Errorf("failed to bulk insert primary keys: %w", err)
	}

	for _, b := range bulk {
		if t.maxVersions > 0 {
			t.db.index.Insert(encodeVersionKey(t.tableID, b.recordID, 1), b.offset)
		}
		t.insertSecondaryKeys(b.record, b.recordID, b.offset)
		if t.db.tx != nil {
			t.db.tx.recordCounts[t.name]++
		} else {
			entry.RecordCount++
		}
	}

	t.db.autoSync()

	ids := make([]uint32, len(bulk))
	for i, b := range bulk {
		ids[i] = b.recordID
	}

	if err := t.db.writeHeader(); err != nil {
		return nil, fmt.Errorf("failed to write header after bulk insert: %w", err)
	}

	t.db.file.Sync()

	return ids, nil
}

// UpdateMany updates multiple records by their primary keys. The updater function is
// called for each record to modify it in place. Returns the number of successfully
// updated records and the first error encountered (if any).
func (t *Table[T]) UpdateMany(ids []any, updater func(*T) error) (int, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	var firstErr error
	updated := 0
	for _, id := range ids {
		record, err := t.getLocked(id)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := updater(record); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := t.updateLocked(id, record); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		updated++
	}
	return updated, firstErr
}

func (t *Table[T]) insertLocked(record *T) (uint32, error) {
	entry := t.db.tableCat[t.name]
	if entry == nil {
		return 0, fmt.Errorf("table not found")
	}

	recordID := entry.NextRecordID
	entry.NextRecordID++

	pkVal, _ := t.getPKValue(record)
	if t.isZeroPK(pkVal) {
		t.setPKValue(record, recordID)
		pkVal = recordID
	} else {
		if _, err := t.db.index.Get(encodePrimaryKey(t.tableID, pkVal)); err == nil {
			return 0, fmt.Errorf("primary key already exists: %v", pkVal)
		}
	}

	recordBuf, err := t.encodeRecord(record, recordID, embeddbcore.FlagsActive, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to encode record: %w", err)
	}

	offset, _, err := t.db.alloc.Allocate(uint64(len(recordBuf)))
	if err != nil {
		return 0, fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(recordBuf, int64(offset)); err != nil {
		return 0, fmt.Errorf("failed to write record: %w", err)
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, pkVal), offset)

	if t.maxVersions > 0 {
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), offset)
	}

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	t.insertSecondaryKeys(record, recordID, offset)

	return recordID, nil
}

func (t *Table[T]) updateLocked(id any, record *T) error {
	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))
	if err != nil {
		return fmt.Errorf("record not found")
	}

	oldOffset := offset

	hdrBuf := make([]byte, embeddbcore.RecordHeaderSize)
	if err := t.db.readAt(hdrBuf, int64(oldOffset)); err != nil {
		return fmt.Errorf("failed to read record header at offset %d: %w", oldOffset, err)
	}

	hdr, err := decodeRecordHeader(hdrBuf)
	if err != nil {
		return fmt.Errorf("invalid record at offset %d: %w", oldOffset, err)
	}

	recordID := hdr.RecordID

	totalLen := recordTotalSize(hdr)
	oldRecordBuf := make([]byte, totalLen)
	copy(oldRecordBuf, hdrBuf)
	if err := t.db.readAt(oldRecordBuf[embeddbcore.RecordHeaderSize:], int64(oldOffset)+int64(embeddbcore.RecordHeaderSize)); err != nil {
		return fmt.Errorf("failed to read old record data: %w", err)
	}

	var oldRecord T
	t.decodeRecord(oldRecordBuf, &oldRecord)

	t.deleteSecondaryKeys(&oldRecord, recordID, oldOffset)

	var flags byte = embeddbcore.FlagsActive
	var prevVersionOff uint64
	if t.maxVersions > 0 {
		flags |= embeddbcore.FlagsHasPrevVersion
		prevVersionOff = oldOffset
	}

	newRecordBuf, err := t.encodeRecord(record, recordID, flags, prevVersionOff)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	newOffset, _, err := t.db.alloc.Allocate(uint64(len(newRecordBuf)))
	if err != nil {
		return fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(newRecordBuf, int64(newOffset)); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	if t.maxVersions > 0 {
		prefix := encodeVersionKeyPrefix(t.tableID, recordID)
		type verInfo struct {
			version uint32
			offset  uint64
		}
		var versions []verInfo
		t.db.index.Scan(func(key []byte, value uint64) bool {
			if bytes.HasPrefix(key, prefix) {
				if _, _, ver, ok := parseVersionKey(key); ok {
					versions = append(versions, verInfo{version: ver, offset: value})
				}
			}
			return true
		})

		if len(versions) == 0 {
			t.db.index.Insert(encodeVersionKey(t.tableID, recordID, 1), oldOffset)
		}
		newVersion := uint32(1)
		for _, v := range versions {
			if v.version >= newVersion {
				newVersion = v.version + 1
			}
		}
		t.db.index.Insert(encodeVersionKey(t.tableID, recordID, newVersion), newOffset)
	} else {
		_ = t.deactivateRecord(oldOffset)
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, t.normalizePK(id)), newOffset)

	t.insertSecondaryKeys(record, recordID, newOffset)

	return nil
}

// Upsert inserts a record if it doesn't exist, or updates it if it does.
// Returns the record ID, a boolean indicating whether the record was inserted (true)
// or updated (false), and any error that occurred.
func (t *Table[T]) Upsert(id any, record *T) (uint32, bool, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	_, err := t.db.index.Get(encodePrimaryKey(t.tableID, t.normalizePK(id)))

	if err == nil {
		if err := t.updateLocked(t.normalizePK(id), record); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}

	entry := t.db.tableCat[t.name]
	if entry == nil {
		return 0, false, fmt.Errorf("table not found")
	}

	recordID := entry.NextRecordID
	entry.NextRecordID++

	t.setPKValue(record, t.normalizePK(id))

	recordBuf, err := t.encodeRecord(record, recordID, embeddbcore.FlagsActive, 0)
	if err != nil {
		return 0, true, fmt.Errorf("failed to encode record: %w", err)
	}

	offset, _, err := t.db.alloc.Allocate(uint64(len(recordBuf)))
	if err != nil {
		return 0, false, fmt.Errorf("failed to allocate space: %w", err)
	}
	if err := t.db.writeAt(recordBuf, int64(offset)); err != nil {
		return 0, false, fmt.Errorf("failed to write record: %w", err)
	}

	pkVal, _ := t.getPKValue(record)
	t.db.index.Insert(encodePrimaryKey(t.tableID, pkVal), offset)

	t.insertSecondaryKeys(record, recordID, offset)

	if t.db.tx != nil {
		t.db.tx.recordCounts[t.name]++
	} else {
		entry.RecordCount++
	}

	t.db.autoSync()

	return recordID, true, nil
}
