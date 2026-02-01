package embeddb

import (
	"encoding/binary"
	"strconv"
	"strings"
)

// Version constant is now defined in main.go

func (db *Database[T]) encodeHeader() error {
	// Prepare byte slice for version (major, minor, patch).
	vb := make([]byte, 3)
	// Initialize the buffer for the full header bytes.
	hb := []byte{}
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
	// Acquire a lock to ensure exclusive access to the database file.
	db.lock.Lock()
	defer db.lock.Unlock()
	// Write the complete header buffer to the beginning of the file (offset 0).
	// The header structure is:
	// [Version (3 bytes)] [tocStart (4 bytes)] [indexStart (4 bytes)] [entryStart (4 bytes)]
	// Total header size written is 3 + 4 + 4 + 4 = 15 bytes.
	_, err := db.file.WriteAt(hb, 0)
	// Release the lock after writing is complete.

	if err != nil {
		return err
	}

	// Return nil to indicate success.
	return nil
}

func (db *Database[T]) decodeHeader() error {
	// Acquire a lock to ensure exclusive access to the database file.
	db.lock.Lock()
	// Prepare a byte slice to read the header. The expected header size is 15 bytes,
	// but we allocate 16 to simplify indexing.
	hb := make([]byte, 16)
	// Read the header bytes from the beginning of the file (offset 0).
	_, err := db.file.ReadAt(hb, 0)
	// Release the lock after reading is complete.
	db.lock.Unlock()
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
	// Ensure the header lock is released when the function exits.
	defer db.header.lock.Unlock()

	// Assign the decoded header values to the database header structure.
	db.header = DBHeader{
		Version:    version,                            // Decoded version string.
		tocStart:   binary.BigEndian.Uint32(hb[3:7]),   // Decode Table of Contents start offset (bytes 3-7).
		indexStart: binary.BigEndian.Uint32(hb[7:11]),  // Decode Index start offset (bytes 7-11).
		entryStart: binary.BigEndian.Uint32(hb[11:15]), // Decode Entry start offset (bytes 11-15).
		// Note: The original header is 15 bytes. The buffer was 16, which is fine for indexing.
	}

	// Return nil to indicate successful decoding.
	return nil
}
