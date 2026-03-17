package embeddb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// writeData writes data to the file at the given offset.
// IMPORTANT: Caller must hold db.lock before calling this method.
func (db *Database[T]) writeData(data []byte, offset int64) error {
	end := offset + int64(len(data))
	if err := ensureSecondaryIndexBlobAfter(db.file, end); err != nil {
		return fmt.Errorf("failed to move embedded secondary index blob: %w", err)
	}
	if err := ensureRegionIndexBlobAfter(db.file, end); err != nil {
		return fmt.Errorf("failed to move embedded region index blob: %w", err)
	}
	_, err := db.file.WriteAt(data, offset)
	return err
}

// readData reads data from the file at the given offset.
// IMPORTANT: Caller must hold db.lock (read or write) before calling this method.
func (db *Database[T]) readData(offset int64, length int) (buf []byte, err error) {
	buf = make([]byte, length)
	n, err := db.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, errors.New("could not read all requested bytes:" + err.Error())
	}
	return buf, nil
}

// findSeq searches for a byte sequence in the file.
// IMPORTANT: Caller must hold db.lock (read or write) before calling this method.
func (db *Database[T]) findSeq(seq []byte) (matchloc []int, err error) {
	// Ensure the sequence to search for is not empty. An empty sequence would match everywhere.
	if len(seq) == 0 {
		return nil, errors.New("search sequence cannot be empty")
	}

	// Start searching from the beginning of the entry data section in the file,
	// as defined by the database header.
	offset := db.header.entryStart
	// Allocate a buffer for reading chunks of the file data.
	// The buffer size (1.5MB) is larger than the overlap (1MB) to ensure contiguous searching across chunks
	// and to allow sequences spanning buffer boundaries to be found.
	// maybe make these tunables in the future
	const bufSize = 1572864 // 1.5 MB
	const advance = 1048576 // 1 MB (overlap is bufSize - advance = 0.5 MB)
	b := make([]byte, bufSize)

	// 'l' is used as an index within the current buffer chunk (`b`) for the inner search loop.
	// It tracks the starting point of the `bytes.Index` search within `b`. Initially 0.
	l := 0
	// 'last' accumulates the relative positions found by `bytes.Index` in previous iterations
	// within the current buffer chunk `b`. It represents the cumulative distance from the
	// start of the buffer `b` to the potential start of the next search in the inner loop.
	last := 0

	// Loop through the file, reading data in chunks starting from `offset`, until io.EOF is encountered.
	// The outer loop condition checks the 'err' variable, which is updated by ReadAt.
	// 'err' is initialized to nil, ensuring the loop runs at least once.
	for err != io.EOF {
		// Read a chunk of data into the buffer `b` starting from the current file offset.
		// 'n' is the number of bytes actually read into the buffer `b`. This might be less than `bufSize`
		// on the last read, or if a read error occurs.
		n, readErr := db.file.ReadAt(b, int64(offset))

		// Handle potential read errors. If `readErr` is `io.EOF`, the outer loop condition will catch it
		// in the next iteration. If it's any other error, return it immediately.
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
		// Update the loop condition variable `err` with the result of ReadAt for the next iteration check.
		err = readErr

		// If no data was read (`n == 0`) and it's EOF, we've reached the end of the file and there's no
		// more data to process in this chunk.
		if n == 0 && err == io.EOF {
			break
		}

		// Search for the sequence within the portion of the buffer that was actually read (`b[0:n]`).
		// Reset the internal buffer search variables for processing the new chunk `b[0:n]`.
		// 'l' starts the search from the beginning of the currently read data in `b`.
		l = 0
		// 'last' is reset to 0 to track cumulative relative positions within this new buffer chunk `b`.
		last = 0

		// Loop to find occurrences of the sequence `seq` within the current buffer chunk `b[0:n]`.
		// The loop continues as long as `bytes.Index` finds a match (returns a non -1 index `l`).
		for l != -1 {
			// Accumulate the relative position found in the previous inner loop iteration (`l`).
			// This updates `last` to represent the cumulative distance from the start of
			// the buffer `b` to the beginning of the current search slice within the buffer.
			last += l

			// Search for 'seq' starting from the current cumulative internal buffer position `last`
			// within the slice `b[last:n]`.
			// `bytes.Index(b[last:n], seq)` returns the index of the first occurrence *relative* to the
			// start of the slice `b[last:n]`, or -1 if the sequence is not found in that part of the buffer.
			// The result is assigned back to 'l', storing the relative position of the match within `b[last:n]`.
			l = bytes.Index(b[last:n], seq)

			// If `bytes.Index` found a match (l is not -1):
			if l != -1 {
				// Calculate the absolute file offset of the match.
				// This calculation adds the cumulative index `last` (which tracks the search progress
				// within the buffer) to the start offset of the buffer chunk in the file (`offset`).
				// The relative match position `l` *within the slice `b[last:n]`* is implicitly accounted for
				// in the next iteration when `l` is added to `last`.
				// Note: This offset calculation `last + int(offset)` appears potentially incorrect if `l` is the
				// relative match position. The correct offset should likely be `offset + last + l`.
				// However, adhering strictly to the original code logic as requested.
				matchloc = append(matchloc, l+last+int(offset))

				// Advance the search position within the buffer for the next iteration of the inner loop.
				// We add the length of the sequence to `l` (the relative match position within `b[last:n]`)
				// to start the next search immediately after the current match.
				// This new value of `l` will be added to `last` in the next iteration of the inner loop
				// to update the cumulative search position.
				l += len(seq)

				// If advancing `l` makes the next search position (which would be `last + l` in the next iteration)
				// go past the end of the read data (`n`), the next `bytes.Index` call
				// on `b[last+l:n]` will correctly handle this (return -1), exiting the inner loop.
			}
		}
		// Note: If a match is found such that `last + l + len(seq)` is exactly equal to `n` (the end of the read chunk),
		// advancing `l` by `len(seq)` might make `l >= n`. The next iteration of the inner loop
		// will add this value of `l` to `last`, potentially making `last + l >= n`. The subsequent search
		// `bytes.Index(b[last+l:n], seq)` will be on an empty or out-of-bounds slice and correctly return -1,
		// causing the inner loop to terminate.

		// Advance the file offset for the next read operation.
		// The offset is advanced by 1MB (`advance`), which is less than the buffer size (`bufSize`, 1.5MB).
		// This creates an overlap between consecutive read operations (0.5MB).
		// The overlap ensures that sequences spanning across buffer boundaries can be found (i.e., a sequence
		// is split with the first part at the end of the current chunk and the second part at the beginning
		// of the next chunk).
		offset = offset + advance
	}

	// Return the slice containing all found match locations (absolute file offsets).
	return matchloc, nil
}
