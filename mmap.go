package embeddb

import (
	"bytes"
	"encoding/binary"
	"errors"

	"golang.org/x/exp/mmap"
)

func (db *Database[T]) ReloadMMap() error {
	db.mlock.Lock()
	defer db.mlock.Unlock()
	if db.mfile != nil {
		db.mfile.Close()
	}
	rp, err := mmap.Open(db.file.Name())
	if err != nil {
		return err
	}
	db.mfile = rp
	return nil
}

func (db *Database[T]) ScanNext(o uint32) (id uint32, buf *bytes.Buffer, active bool, err error) {
	// If the memory-mapped file is not open, attempt to reload it.
	if db.mfile == nil {
		db.ReloadMMap()
	}

	// Acquire a read lock on the mmap file to ensure data consistency during read operations.
	db.mlock.RLock()
	defer db.mlock.RUnlock()

	// Create a byte slice to hold the entire mmap file's content.
	data := make([]byte, db.mfile.Len())
	// Read the content of the mmap file into the data slice, starting from entryStart.
	// This reads the entire data section, which might be inefficient for very large files.
	_, err = db.mfile.ReadAt(data, 16)
	if err != nil {
		return 0, nil, false, err
	}

	// Search for the start marker sequence from the given offset 'o'.
	// bytes.Index returns the index relative to the slice `data[o:]`.
	// We then add 'o' to get the absolute index in `data`.
	start := uint32(bytes.Index(data[o:], []byte{escCode, startMarker})) + o

	// Check the 'active' byte in the record header.
	// The active byte is at offset `start + 11` (new format: 2 for markers + 1 for tableID + 4 for id + 4 for length).
	if db.mfile.At(int(start+11)) == byte(1) {
		active = true
	} else {
		active = false
	}

	// Extract the 'id' (4 bytes) from the record header.
	// ID is located at `start + 3` (after the 2-byte start marker and 1-byte tableID).
	id = binary.BigEndian.Uint32(data[start+3 : start+7])

	// Extract the 'length' (4 bytes) of the record data.
	// Length is located at `start + 7` (after the 2-byte start marker + 1-byte tableID + 4-byte ID).
	length := binary.BigEndian.Uint32(data[start+7 : start+11])

	// Search for the end marker sequence, starting from the current record's start.
	end := uint32(bytes.Index(data[start:], []byte{escCode, endMarker})) + start // Add 'start' to get absolute index

	// Validate the record: The calculated length (end - (start + 12)) should match the stored 'length'.
	// `start + 12` is the beginning of the actual data payload:
	// 2 (start marker) + 1 (tableID) + 4 (id) + 4 (length) + 1 (active byte) = 12 bytes.
	if (end - (start + 12)) != length {
		return 0, nil, false, errors.New("record invalid: length mismatch")
	}

	// Create a new bytes.Buffer containing the raw record data, including markers and headers.
	buf = bytes.NewBuffer(data[start+12 : end])
	return id, buf, active, nil
}
