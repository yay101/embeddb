package embeddb

import (
	"bytes"
	"fmt"

	"github.com/yay101/embeddbcore"
)

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

func (t *Table[T]) Get(id any) (*T, error) {
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

	return t.getLocked(t.normalizePK(id))
}

func (t *Table[T]) getLocked(pkValue any) (*T, error) {
	offset, err := t.db.index.Get(encodePrimaryKey(t.tableID, pkValue))
	if err != nil {
		return nil, fmt.Errorf("record not found")
	}

	return t.readRecordAt(offset)
}

func (t *Table[T]) GetVersion(id any, version uint32) (*T, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

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

func (t *Table[T]) ListVersions(id any) ([]VersionMetadata, error) {
	if t.maxVersions == 0 {
		return nil, fmt.Errorf("versioning not enabled for this table")
	}

	t.db.mu.RLock()
	defer t.db.mu.RUnlock()

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
			t.deactivateRecord(versions[oldestIdx].offset)
			t.db.index.Delete(encodeVersionKey(t.tableID, recordID, versions[oldestIdx].version))
			versions = append(versions[:oldestIdx], versions[oldestIdx+1:]...)
		}
	} else {
		t.deactivateRecord(offset)
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, t.normalizePK(id)), newOffset)

	t.insertSecondaryKeys(record, recordID, newOffset)

	t.db.autoSync()

	return nil
}

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

	t.deactivateRecord(offset)

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

		t.deactivateRecord(offset)
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
		t.deactivateRecord(oldOffset)
	}

	t.db.index.Insert(encodePrimaryKey(t.tableID, t.normalizePK(id)), newOffset)

	t.insertSecondaryKeys(record, recordID, newOffset)

	return nil
}

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
