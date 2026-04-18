package embeddb

import (
	"bytes"
	"fmt"

	"github.com/yay101/embeddbcore"
)

func migrateTable(db *database, table *tableCatalogEntry, newLayout *embeddbcore.StructLayout) error {
	type migrateRecord struct {
		key    []byte
		offset uint64
	}

	var records []migrateRecord

	pkPrefix := []byte{indexNSPrimary, table.ID}
	db.index.Scan(func(key []byte, value uint64) bool {
		if bytes.HasPrefix(key, pkPrefix) {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			records = append(records, migrateRecord{
				key:    keyCopy,
				offset: value,
			})
		}
		return true
	})

	if len(records) == 0 {
		return nil
	}

	type pendingWrite struct {
		newRecord []byte
		newOffset uint64
		oldOffset uint64
		key       []byte
	}

	var pending []pendingWrite
	var maxOffset uint64 = FileHeaderSize

	for _, rec := range records {
		hdrBuf := make([]byte, embeddbcore.RecordHeaderSize)
		if err := db.readAt(hdrBuf, int64(rec.offset)); err != nil {
			continue
		}
		if hdrBuf[0] != V2RecordVersion {
			continue
		}

		hdr, err := decodeRecordHeader(hdrBuf)
		if err != nil {
			continue
		}

		if !hdr.IsActive() {
			continue
		}

		totalLen := recordTotalSize(hdr)
		recData := make([]byte, totalLen)
		copy(recData, hdrBuf)
		if err := db.readAt(recData[embeddbcore.RecordHeaderSize:], int64(rec.offset)+int64(embeddbcore.RecordHeaderSize)); err != nil {
			continue
		}

		_, payload, err := parseV2Record(recData)
		if err != nil {
			continue
		}

		var newPayload []byte
		for len(payload) > 0 {
			name, value, remaining, err := embeddbcore.DecodeTLVField(payload)
			if err != nil {
				break
			}
			payload = remaining

			found := false
			for _, field := range newLayout.Fields {
				if field.Name == name {
					found = true
					break
				}
			}
			if found {
				newPayload = embeddbcore.EncodeTLVField(newPayload, name, value)
			}
		}

		if len(newPayload) == 0 {
			newPayload = []byte{}
		}

		newRecord := buildV2Record(table.ID, hdr.RecordID, newLayout.SchemaVersion, hdr.Flags, hdr.PrevVersionOff, newPayload)

		newOffset, _, err := db.alloc.Allocate(uint64(len(newRecord)))
		if err != nil {
			return fmt.Errorf("migration allocate failed: %w", err)
		}

		pending = append(pending, pendingWrite{
			newRecord: newRecord,
			newOffset: newOffset,
			oldOffset: rec.offset,
			key:       rec.key,
		})

		endOffset := newOffset + uint64(len(newRecord))
		if endOffset > maxOffset {
			maxOffset = endOffset
		}
	}

	for _, pw := range pending {
		if err := db.writeAt(pw.newRecord, int64(pw.newOffset)); err != nil {
			return fmt.Errorf("migration write failed: %w", err)
		}
	}

	for _, pw := range pending {
		deactivateBuf := make([]byte, embeddbcore.RecordHeaderSize)
		db.readAt(deactivateBuf, int64(pw.oldOffset))
		deactivateBuf[1] &^= embeddbcore.FlagsActive
		db.writeAt(deactivateBuf, int64(pw.oldOffset))

		db.index.Insert(pw.key, pw.newOffset)
	}

	db.alloc.Reset(maxOffset, nil)

	return nil
}
