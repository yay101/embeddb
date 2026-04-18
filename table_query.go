package embeddb

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	embedcore "github.com/yay101/embeddbcore"
)

func (t *Table[T]) Query(fieldName string, value interface{}) ([]T, error) {
	if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][fieldName] {
		return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
	}

	autoIndex := t.db.parent != nil && t.db.parent.autoIndex
	explicitAllowed := false
	if !autoIndex && t.db.explicitIndexes != nil {
		for _, f := range t.db.explicitIndexes[t.name] {
			if f == fieldName {
				explicitAllowed = true
				break
			}
		}
	}

	if autoIndex || explicitAllowed {
		field, err := t.findField(fieldName)
		if err == nil && !field.Primary && field.Offset > 0 {
			strValue := valueToIndexKey(value, field.Type)
			prefix := encodeSecondaryKeyPrefixWithValue(t.tableID, fieldName, strValue)

			var offsets []uint64
			_ = t.db.index.Scan(func(key []byte, val uint64) bool {
				if bytes.HasPrefix(key, prefix) {
					offsets = append(offsets, val)
				}
				return true
			})

			slices.Sort(offsets)
			results := make([]T, 0, len(offsets))

			t.db.mu.RLock()
			for _, off := range offsets {
				record, err := t.readRecordAt(off)
				if err == nil && record != nil {
					results = append(results, *record)
				}
			}
			t.db.mu.RUnlock()

			return results, nil
		}
	}

	if strings.Contains(fieldName, ".") {
		comparator := func(fieldValue interface{}) bool {
			return compareValues(fieldValue, value) == 0
		}
		return t.filterByField(fieldName, comparator)
	}

	return nil, fmt.Errorf("no index exists for field '%s'", fieldName)
}

func (t *Table[T]) QueryRangeGreaterThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if t.canUseIndex(fieldName) {
		return t.rangeQueryFromField(fieldName, value, inclusive, true)
	}
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) > 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryRangeLessThan(fieldName string, value interface{}, inclusive bool) ([]T, error) {
	if t.canUseIndex(fieldName) {
		return t.rangeQueryToField(fieldName, value, inclusive)
	}
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) < 0 || (inclusive && compareValues(fieldValue, value) == 0)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryRangeBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
	if t.canUseIndex(fieldName) {
		return t.rangeQueryBetween(fieldName, min, max, inclusiveMin, inclusiveMax)
	}
	comparator := func(fieldValue interface{}) bool {
		cMin := compareValues(fieldValue, min)
		cMax := compareValues(fieldValue, max)
		aboveMin := cMin > 0 || (inclusiveMin && cMin == 0)
		belowMax := cMax < 0 || (inclusiveMax && cMax == 0)
		return aboveMin && belowMax
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryNotEqual(fieldName string, value interface{}) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) != 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryGreaterOrEqual(fieldName string, value interface{}) ([]T, error) {
	if t.canUseIndex(fieldName) {
		return t.rangeQueryFromField(fieldName, value, true, true)
	}
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) >= 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryLessOrEqual(fieldName string, value interface{}) ([]T, error) {
	if t.canUseIndex(fieldName) {
		return t.rangeQueryToField(fieldName, value, true)
	}
	comparator := func(fieldValue interface{}) bool {
		return compareValues(fieldValue, value) <= 0
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryLike(fieldName string, pattern string) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		str, ok := fieldValue.(string)
		if !ok {
			return false
		}
		return matchLike(str, pattern)
	}
	return t.filterByField(fieldName, comparator)
}

func (t *Table[T]) QueryNotLike(fieldName string, pattern string) ([]T, error) {
	comparator := func(fieldValue interface{}) bool {
		str, ok := fieldValue.(string)
		if !ok {
			return false
		}
		return !matchLike(str, pattern)
	}
	return t.filterByField(fieldName, comparator)
}

func matchLike(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}
	if pattern == "%" {
		return true
	}
	sLower := strings.ToLower(s)
	patternLower := strings.ToLower(pattern)
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		return strings.Contains(sLower, strings.Trim(patternLower, "%"))
	}
	if strings.HasPrefix(pattern, "%") {
		return strings.HasSuffix(sLower, strings.Trim(patternLower, "%"))
	}
	if strings.HasSuffix(pattern, "%") {
		return strings.HasPrefix(sLower, strings.Trim(patternLower, "%"))
	}
	return sLower == patternLower
}

func (t *Table[T]) canUseIndex(fieldName string) bool {
	if strings.Contains(fieldName, ".") {
		return false
	}
	if t.db.droppedIndexes != nil && t.db.droppedIndexes[t.name] != nil && t.db.droppedIndexes[t.name][fieldName] {
		return false
	}
	if t.db.parent != nil && t.db.parent.autoIndex {
		field, err := t.findField(fieldName)
		return err == nil && !field.Primary && field.Offset > 0 && !field.IsSlice
	}
	if t.db.explicitIndexes != nil {
		for _, f := range t.db.explicitIndexes[t.name] {
			if f == fieldName {
				return true
			}
		}
	}
	return false
}

func (t *Table[T]) rangeQueryFromField(fieldName string, startValue interface{}, inclusive bool, fromField bool) ([]T, error) {
	field, err := t.findField(fieldName)
	if err != nil {
		return nil, err
	}

	prefix := encodeSecondaryKeyPrefix(t.tableID, fieldName)

	cmpFunc := func(fieldValue any) bool {
		return compareValues(fieldValue, startValue) > 0 || (inclusive && compareValues(fieldValue, startValue) == 0)
	}

	var offsets []uint64
	t.db.mu.RLock()
	_ = t.db.index.Scan(func(key []byte, val uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			_, _, fv, _, parsed := parseSecondaryKey(key)
			if parsed {
				fieldVal := decodeIndexKeyToValue(fv, field.Type)
				if cmpFunc(fieldVal) {
					offsets = append(offsets, val)
				}
			}
		}
		return true
	})
	t.db.mu.RUnlock()

	slices.Sort(offsets)
	results := make([]T, 0, len(offsets))
	t.db.mu.RLock()
	for _, off := range offsets {
		record, err := t.readRecordAt(off)
		if err == nil && record != nil {
			results = append(results, *record)
		}
	}
	t.db.mu.RUnlock()

	return results, nil
}

func (t *Table[T]) rangeQueryToField(fieldName string, endValue interface{}, inclusive bool) ([]T, error) {
	field, err := t.findField(fieldName)
	if err != nil {
		return nil, err
	}

	prefix := encodeSecondaryKeyPrefix(t.tableID, fieldName)

	cmpFunc := func(fieldValue any) bool {
		return compareValues(fieldValue, endValue) < 0 || (inclusive && compareValues(fieldValue, endValue) == 0)
	}

	var offsets []uint64
	t.db.mu.RLock()
	_ = t.db.index.Scan(func(key []byte, val uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			_, _, fv, _, parsed := parseSecondaryKey(key)
			if parsed {
				fieldVal := decodeIndexKeyToValue(fv, field.Type)
				if cmpFunc(fieldVal) {
					offsets = append(offsets, val)
				}
			}
		}
		return true
	})
	t.db.mu.RUnlock()

	slices.Sort(offsets)
	results := make([]T, 0, len(offsets))
	t.db.mu.RLock()
	for _, off := range offsets {
		record, err := t.readRecordAt(off)
		if err == nil && record != nil {
			results = append(results, *record)
		}
	}
	t.db.mu.RUnlock()

	return results, nil
}

func (t *Table[T]) rangeQueryBetween(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool) ([]T, error) {
	field, err := t.findField(fieldName)
	if err != nil {
		return nil, err
	}

	prefix := encodeSecondaryKeyPrefix(t.tableID, fieldName)

	cmpFunc := func(fieldValue any) bool {
		cMin := compareValues(fieldValue, min)
		cMax := compareValues(fieldValue, max)
		aboveMin := cMin > 0 || (inclusiveMin && cMin == 0)
		belowMax := cMax < 0 || (inclusiveMax && cMax == 0)
		return aboveMin && belowMax
	}

	var offsets []uint64
	t.db.mu.RLock()
	_ = t.db.index.Scan(func(key []byte, val uint64) bool {
		if bytes.HasPrefix(key, prefix) {
			_, _, fv, _, parsed := parseSecondaryKey(key)
			if parsed {
				fieldVal := decodeIndexKeyToValue(fv, field.Type)
				if cmpFunc(fieldVal) {
					offsets = append(offsets, val)
				}
			}
		}
		return true
	})
	t.db.mu.RUnlock()

	slices.Sort(offsets)
	results := make([]T, 0, len(offsets))
	t.db.mu.RLock()
	for _, off := range offsets {
		record, err := t.readRecordAt(off)
		if err == nil && record != nil {
			results = append(results, *record)
		}
	}
	t.db.mu.RUnlock()

	return results, nil
}

func incrementIndexValue(v []byte) []byte {
	result := make([]byte, len(v))
	copy(result, v)
	for i := len(result) - 1; i >= 0; i-- {
		result[i]++
		if result[i] != 0 {
			return result
		}
	}
	return append(result, 0)
}

func decrementIndexValue(v []byte) []byte {
	if len(v) == 0 {
		return v
	}
	result := make([]byte, len(v))
	copy(result, v)
	for i := len(result) - 1; i >= 0; i-- {
		result[i]--
		if result[i] != 0xFF {
			if i == len(result)-1 && result[i] == 0 {
				return result[:len(result)-1]
			}
			return result
		}
	}
	return result
}

func decodeIndexKeyToValue(data []byte, kind reflect.Kind) any {
	s := valueToIndexKeyReverse(data)
	if s == "" && len(data) > 0 && data[0] != 0x02 {
		return int(0)
	}
	switch kind {
	case reflect.Int:
		v, _ := strconv.ParseInt(s, 10, 64)
		return int(v)
	case reflect.Int8:
		v, _ := strconv.ParseInt(s, 10, 8)
		return int8(v)
	case reflect.Int16:
		v, _ := strconv.ParseInt(s, 10, 16)
		return int16(v)
	case reflect.Int32:
		v, _ := strconv.ParseInt(s, 10, 32)
		return int32(v)
	case reflect.Int64:
		v, _ := strconv.ParseInt(s, 10, 64)
		return v
	case reflect.Uint:
		v, _ := strconv.ParseUint(s, 10, 64)
		return uint(v)
	case reflect.Uint8:
		v, _ := strconv.ParseUint(s, 10, 8)
		return uint8(v)
	case reflect.Uint16:
		v, _ := strconv.ParseUint(s, 10, 16)
		return uint16(v)
	case reflect.Uint32:
		v, _ := strconv.ParseUint(s, 10, 32)
		return uint32(v)
	case reflect.Uint64:
		v, _ := strconv.ParseUint(s, 10, 64)
		return v
	case reflect.Float32:
		v, _ := strconv.ParseFloat(s, 32)
		return float32(v)
	case reflect.Float64:
		v, _ := strconv.ParseFloat(s, 64)
		return v
	case reflect.String:
		return s
	case reflect.Bool:
		return s == "true" || s == "1"
	case reflect.Struct:
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return time.Unix(0, n)
		}
	}
	return nil
}

func valueToIndexKeyReverse(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	s, _, err := embedcore.DecodeString(data)
	if err == nil {
		return s
	}
	var buf []byte
	v, _, err2 := embedcore.DecodeVarint(data)
	if err2 == nil {
		buf = strconv.AppendInt(buf, v, 10)
	} else {
		uv, _, err3 := embedcore.DecodeUvarint(data)
		if err3 == nil {
			buf = strconv.AppendUint(buf, uv, 10)
		}
	}
	return string(buf)
}

func (t *Table[T]) filterByField(fieldName string, fn func(interface{}) bool) ([]T, error) {
	field, err := t.findField(fieldName)
	if err != nil {
		return nil, err
	}

	scanner := t.ScanRecords()
	defer scanner.Close()

	results := make([]T, 0)
	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		fieldVal, err := embedcore.GetFieldValue(record, field)
		if err != nil {
			continue
		}
		if fn(fieldVal) {
			results = append(results, *record)
		}
	}
	return results, nil
}

func (t *Table[T]) filterPagedByField(field embedcore.FieldOffset, fn func(interface{}) bool, offset, limit int) (*PagedResult[T], error) {
	scanner := t.ScanRecords()
	defer scanner.Close()

	var results []T
	totalCount := 0
	skipped := 0

	for scanner.Next() {
		record, err := scanner.Record()
		if err != nil {
			continue
		}
		fieldVal, err := embedcore.GetFieldValue(record, field)
		if err != nil {
			continue
		}
		if fn(fieldVal) {
			totalCount++
			if skipped < offset {
				skipped++
				continue
			}
			if len(results) < limit {
				results = append(results, *record)
			}
		}
	}

	if results == nil {
		results = []T{}
	}

	hasMore := totalCount > offset+len(results)
	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    hasMore,
	}, nil
}

func (t *Table[T]) findField(fieldName string) (embedcore.FieldOffset, error) {
	for _, f := range t.layout.Fields {
		if f.Name == fieldName {
			return f, nil
		}
	}
	return embedcore.FieldOffset{}, fmt.Errorf("field %s not found", fieldName)
}

func compareValues(a, b interface{}) int {
	if aTime, ok := a.(time.Time); ok {
		bTime, ok := b.(time.Time)
		if !ok {
			return 0
		}
		if aTime.Before(bTime) {
			return -1
		} else if aTime.After(bTime) {
			return 1
		}
		return 0
	}

	aVal := reflect.ValueOf(a)

	switch aVal.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		aInt := aVal.Int()
		bInt := toInt64(b)
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		aUint := aVal.Uint()
		bUint := toUint64(b)
		if aUint < bUint {
			return -1
		} else if aUint > bUint {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		aFloat := aVal.Float()
		bFloat := toFloat64(b)
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	case reflect.String:
		aStr := aVal.String()
		bStr, _ := b.(string)
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}
	return 0
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	}
	return 0
}

func toUint64(v interface{}) uint64 {
	switch val := v.(type) {
	case int:
		return uint64(val)
	case int8:
		return uint64(val)
	case int16:
		return uint64(val)
	case int32:
		return uint64(val)
	case int64:
		return uint64(val)
	case uint:
		return uint64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint64:
		return val
	case float32:
		return uint64(val)
	case float64:
		return uint64(val)
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	}
	return 0
}

func parseIndexKey(key string, fieldKind reflect.Kind) interface{} {
	switch fieldKind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v, err := strconv.ParseInt(key, 10, 64); err == nil {
			return v
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v, err := strconv.ParseUint(key, 10, 64); err == nil {
			return v
		}
	case reflect.Float32, reflect.Float64:
		if v, err := strconv.ParseFloat(key, 64); err == nil {
			return v
		}
	case reflect.String:
		return key
	case reflect.Bool:
		if v, err := strconv.ParseBool(key); err == nil {
			return v
		}
	case reflect.Struct:
		if v, err := strconv.ParseInt(key, 10, 64); err == nil {
			return time.Unix(0, v).UTC()
		}
	}
	return key
}

func valueToIndexKey(val any, fieldKind reflect.Kind) string {
	switch v := val.(type) {
	case time.Time:
		return strconv.FormatInt(v.UnixNano(), 10)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	}
	return fmt.Sprintf("%v", val)
}
