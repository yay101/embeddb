package embeddb

import (
	"reflect"

	"github.com/yay101/embeddbcore"
)

type scanEntry struct {
	pkValue any
}

// Scanner provides sequential iteration over records in a table with early exit support.
// Use ScanRecords() to create a scanner, then call Next() to advance and Record() to
// get the current record. Close() should be called when done to release resources.
type Scanner[T any] struct {
	table   *Table[T]
	entries []scanEntry
	pos     int
	current *T
	err     error
	locked  bool
}

// ScanRecords creates a Scanner that iterates over all records in the table.
func (t *Table[T]) ScanRecords() *Scanner[T] {
	var entries []scanEntry
	_ = t.db.index.Scan(func(key []byte, value uint64) bool {
		if len(key) >= 2 && key[0] == indexNSPrimary && key[1] == t.tableID {
			pkBytes := key[2:]
			var pkVal any
			switch t.layout.PKType {
			case reflect.String:
				s, _, _ := embeddbcore.DecodeString(pkBytes)
				pkVal = s
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v, _, _ := embeddbcore.DecodeVarint(pkBytes)
				pkVal = int(v)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v, _, _ := embeddbcore.DecodeUvarint(pkBytes)
				pkVal = uint64(v)
			default:
				v, _, _ := embeddbcore.DecodeUvarint(pkBytes)
				pkVal = uint64(v)
			}
			entries = append(entries, scanEntry{pkValue: pkVal})
		}
		return true
	})

	return &Scanner[T]{
		table:   t,
		entries: entries,
		pos:     0,
		locked:  false,
	}
}

// Next advances the scanner to the next valid record. Returns false when no more
// records are available or an error occurred.
func (s *Scanner[T]) Next() bool {
	for {
		if s.err != nil || s.pos >= len(s.entries) {
			return false
		}

		record, err := s.table.getLocked(s.entries[s.pos].pkValue)
		s.pos++
		if err != nil {
			continue
		}

		s.current = record
		return true
	}
}

// Record returns the current record. Call Next() first to position the scanner.
func (s *Scanner[T]) Record() (*T, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.current, nil
}

// Err returns any error that occurred during scanning.
func (s *Scanner[T]) Err() error {
	return s.err
}

// Close releases the scanner's resources. The scanner should not be used after Close.
func (s *Scanner[T]) Close() {
	s.entries = nil
}

// All returns all records in the table as a slice.
func (t *Table[T]) All() ([]T, error) {
	scanner := t.ScanRecords()
	defer scanner.Close()

	results := make([]T, 0)

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		results = append(results, *record)
	}

	return results, scanner.Err()
}

// Filter returns all records that match the given predicate function.
func (t *Table[T]) Filter(fn func(T) bool) ([]T, error) {
	scanner := t.ScanRecords()
	defer scanner.Close()

	results := make([]T, 0)

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}

		if fn(*record) {
			results = append(results, *record)
		}
	}

	return results, scanner.Err()
}

// Scan iterates over all records, calling fn for each. If fn returns false, iteration stops.
func (t *Table[T]) Scan(fn func(T) bool) error {
	scanner := t.ScanRecords()
	defer scanner.Close()

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}

		if !fn(*record) {
			break
		}
	}

	return scanner.Err()
}

// Count returns the number of active records in the table.
func (t *Table[T]) Count() int {
	t.db.mu.RLock()
	entry := t.db.tableCat[t.name]
	if entry != nil {
		count := int(entry.RecordCount)
		t.db.mu.RUnlock()
		return count
	}
	t.db.mu.RUnlock()
	count := 0
	_ = t.db.index.Scan(func(key []byte, value uint64) bool {
		if len(key) >= 2 && key[0] == indexNSPrimary && key[1] == t.tableID {
			count++
		}
		return true
	})
	return count
}
