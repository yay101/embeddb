package embeddb

import (
	"encoding/binary"
	"strconv"
	"strings"
)

// Version constant is now defined in main.go

// Header format (expanded to 52 bytes):
// [Version (3 bytes)] [tocStart (4 bytes)] [indexStart (4 bytes)] [entryStart (4 bytes)]
// [nextOffset (4 bytes)] [nextRecordID (4 bytes)] [indexCapacity (4 bytes)] [lgIndexStart (4 bytes)]
// [indexEnd (4 bytes)] [tableCatalogOffset (4 bytes)] [tableCount (4 bytes)] [reserved (3 bytes)]
// Total: 52 bytes

// encodeHeader writes the header to the database file.
// This is the public API that acquires locks.
func (db *Database[T]) encodeHeader() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.encodeHeaderLocked()
}

// encodeHeaderLocked writes the header to the database file.
// IMPORTANT: Caller must hold db.lock before calling this method.
func (db *Database[T]) encodeHeaderLocked() error {
	// Prepare byte slice for version (major, minor, patch).
	vb := make([]byte, 3)
	// Initialize the buffer for the full header bytes.
	hb := make([]byte, 0, headerSize)
	// Prepare byte slice for 32-bit unsigned integers (used for offsets).
	ub := make([]byte, 4)

	// Split the version string (e.g., "0.0.1") into parts.
	strsli := strings.Split(Version, ".")
	// Convert each part of the version string to an integer and store as a byte.
	for i := range strsli {
		n, err := strconv.Atoi(strsli[i])
		if err != nil {
			return err
		}
		vb[i] = uint8(n)
	}
	// Append the version bytes (3 bytes) to the header buffer.
	hb = append(hb, vb...)

	// Encode the Table of Contents start offset (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.tocStart)
	// Append the encoded tocStart (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the Index start offset (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.indexStart)
	// Append the encoded indexStart (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the Entry start offset (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.entryStart)
	// Append the encoded entryStart (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the nextOffset (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.nextOffset)
	// Append the encoded nextOffset (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the nextRecordID (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.nextRecordID)
	// Append the encoded nextRecordID (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the indexCapacity (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.indexCapacity)
	// Append the encoded indexCapacity (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the lgIndexStart (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.lgIndexStart)
	// Append the encoded lgIndexStart (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the indexEnd (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.indexEnd)
	// Append the encoded indexEnd (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the tableCatalogOffset (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.tableCatalogOffset)
	// Append the encoded tableCatalogOffset (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Encode the tableCount (uint32) in BigEndian.
	binary.BigEndian.PutUint32(ub, db.header.tableCount)
	// Append the encoded tableCount (4 bytes) to the header buffer.
	hb = append(hb, ub...)

	// Add reserved bytes for future expansion (7 bytes)
	for i := 0; i < 7; i++ {
		hb = append(hb, 0)
	}

	// Write the complete header buffer to the beginning of the file (offset 0).
	// Total header size written is 48 bytes.
	_, err := db.file.WriteAt(hb, 0)

	if err != nil {
		return err
	}

	// Return nil to indicate success.
	return nil
}

// decodeHeader reads the header from the database file.
// This is the public API that acquires locks.
func (db *Database[T]) decodeHeader() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.decodeHeaderLocked()
}

// decodeHeaderLocked reads the header from the database file.
// IMPORTANT: Caller must hold db.lock before calling this method.
func (db *Database[T]) decodeHeaderLocked() error {
	// Prepare a byte slice to read the header (32 bytes).
	hb := make([]byte, headerSize)
	// Read the header bytes from the beginning of the file (offset 0).
	_, err := db.file.ReadAt(hb, 0)
	if err != nil {
		// Return the error if reading fails.
		return err
	}

	// Decode the version string (first 3 bytes).
	// The version is stored as three separate bytes (major, minor, patch).
	bp := 0                  // Byte pointer/offset within the header buffer.
	versionsli := []string{} // Slice to hold version parts as strings.
	// Iterate through the first 3 bytes.
	for bp <= 2 {
		// Convert each byte to an integer and then to a string.
		versionsli = append(versionsli, strconv.Itoa(int(hb[bp])))
		bp++ // Move to the next byte.
	}
	// Join the version parts with dots to form the version string (e.g., "0.0.1").
	version := strings.Join(versionsli, ".")

	// Lock the header for writing.
	db.header.lock.Lock()
	defer db.header.lock.Unlock()

	// Update header fields individually to preserve the mutex
	// DO NOT replace the entire struct as that would replace the mutex
	db.header.Version = version
	db.header.tocStart = binary.BigEndian.Uint32(hb[3:7])
	db.header.indexStart = binary.BigEndian.Uint32(hb[7:11])
	db.header.entryStart = binary.BigEndian.Uint32(hb[11:15])
	db.header.nextOffset = binary.BigEndian.Uint32(hb[15:19])
	db.header.indexCapacity = binary.BigEndian.Uint32(hb[23:27])
	db.header.lgIndexStart = binary.BigEndian.Uint32(hb[27:31])
	db.header.indexEnd = binary.BigEndian.Uint32(hb[31:35])
	db.header.tableCatalogOffset = binary.BigEndian.Uint32(hb[35:39])
	db.header.tableCount = binary.BigEndian.Uint32(hb[39:43])

	// Read nextRecordID and set it on the database
	db.nextRecordID = binary.BigEndian.Uint32(hb[19:23])

	// If nextOffset is 0 (old format), initialize it to after header
	if db.header.nextOffset == 0 {
		db.header.nextOffset = uint32(headerSize)
	}

	// If nextRecordID is 0, start at 1
	if db.nextRecordID == 0 {
		db.nextRecordID = 1
	}

	// Return nil to indicate successful decoding.
	return nil
}
