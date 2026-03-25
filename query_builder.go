package embeddb

import (
	"fmt"
	"reflect"
	"strings"
)

type Operator string

const (
	OpExact   Operator = "="
	OpGT      Operator = ">"
	OpGTE     Operator = ">="
	OpLT      Operator = "<"
	OpLTE     Operator = "<="
	OpNotEq   Operator = "!="
	OpBetween Operator = "BETWEEN"
	OpLike    Operator = "LIKE"
)

type Condition struct {
	Field    string
	Operator Operator
	Value    any
	Value2   any
	IsLimit  bool
	IsOffset bool
	Limit    int
	Offset   int
}

func parseCondition(field, value string) (*Condition, error) {
	c := &Condition{Field: field}

	if field == "_limit" {
		var limit int
		_, err := fmt.Sscanf(value, "%d", &limit)
		if err != nil {
			return nil, fmt.Errorf("invalid _limit value: %s", value)
		}
		c.IsLimit = true
		c.Limit = limit
		return c, nil
	}

	if field == "_offset" {
		var offset int
		_, err := fmt.Sscanf(value, "%d", &offset)
		if err != nil {
			return nil, fmt.Errorf("invalid _offset value: %s", value)
		}
		c.IsOffset = true
		c.Offset = offset
		return c, nil
	}

	if !strings.HasPrefix(value, "{") || !strings.HasSuffix(value, "}") {
		c.Operator = OpExact
		c.Value = parseValue(value)
		return c, nil
	}

	content := value[1 : len(value)-1]
	content = strings.TrimSpace(content)

	if strings.HasPrefix(content, "BETWEEN") || strings.HasPrefix(content, "between") {
		parts := strings.Split(content, "AND")
		if len(parts) != 2 {
			return nil, fmt.Errorf("BETWEEN requires exactly 2 values: %s", value)
		}
		c.Operator = OpBetween
		c.Value = parseValue(strings.TrimSpace(parts[0][7:]))
		c.Value2 = parseValue(strings.TrimSpace(parts[1]))
		return c, nil
	}

	if strings.HasPrefix(content, "LIKE") || strings.HasPrefix(content, "like") {
		c.Operator = OpLike
		c.Value = strings.TrimSpace(content[4:])
		return c, nil
	}

	remaining := strings.TrimSpace(content)
	var opStr string
	var valStr string

	if len(remaining) >= 2 {
		switch remaining[:2] {
		case ">=":
			opStr = ">="
			valStr = strings.TrimSpace(remaining[2:])
		case "<=":
			opStr = "<="
			valStr = strings.TrimSpace(remaining[2:])
		case "!=":
			opStr = "!="
			valStr = strings.TrimSpace(remaining[2:])
		}
	}

	if opStr == "" && len(remaining) >= 1 {
		switch remaining[0] {
		case '>':
			opStr = ">"
			valStr = strings.TrimSpace(remaining[1:])
		case '<':
			opStr = "<"
			valStr = strings.TrimSpace(remaining[1:])
		case '=':
			opStr = "="
			valStr = strings.TrimSpace(remaining[1:])
		}
	}

	if opStr == "" {
		return nil, fmt.Errorf("unknown operator in: %s", value)
	}

	switch opStr {
	case ">":
		c.Operator = OpGT
	case ">=":
		c.Operator = OpGTE
	case "<":
		c.Operator = OpLT
	case "<=":
		c.Operator = OpLTE
	case "!=":
		c.Operator = OpNotEq
	case "=":
		c.Operator = OpExact
	default:
		return nil, fmt.Errorf("unsupported operator: %s", opStr)
	}

	c.Value = parseValue(valStr)
	return c, nil
}

func parseValue(s string) any {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	var val int
	if _, err := fmt.Sscanf(s, "%d", &val); err == nil {
		return val
	}

	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
		return f
	}

	if strings.EqualFold(s, "true") {
		return true
	}
	if strings.EqualFold(s, "false") {
		return false
	}

	return s
}

func (t *Table[T]) QueryBy(conditions map[string]any) (*PagedResult[T], error) {
	var parsedConditions []*Condition
	limit := 0
	offset := 0

	for field, value := range conditions {
		valueStr, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("condition values must be strings, got %T for field %s", value, field)
		}

		cond, err := parseCondition(field, valueStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse condition for %s: %w", field, err)
		}

		if cond.IsLimit {
			limit = cond.Limit
			continue
		}
		if cond.IsOffset {
			offset = cond.Offset
			continue
		}

		parsedConditions = append(parsedConditions, cond)
	}

	var indexedExact []*Condition
	var nonIndexed []*Condition

	for _, cond := range parsedConditions {
		if !t.indexManager.HasIndex(cond.Field) || cond.Operator == OpLike {
			nonIndexed = append(nonIndexed, cond)
			continue
		}

		switch cond.Operator {
		case OpExact:
			indexedExact = append(indexedExact, cond)
		case OpGT, OpGTE, OpLT, OpLTE, OpNotEq, OpBetween:
			nonIndexed = append(nonIndexed, cond)
		default:
			nonIndexed = append(nonIndexed, cond)
		}
	}

	var recordIDs []uint32

	// Handle case with no conditions (just pagination) - return all with pagination
	if len(indexedExact) == 0 && len(nonIndexed) == 0 {
		return t.AllPaged(offset, limit)
	}

	if len(indexedExact) > 0 {
		var err error
		recordIDs, err = t.indexManager.Query(indexedExact[0].Field, indexedExact[0].Value)
		if err != nil {
			return nil, err
		}

		for i := 1; i < len(indexedExact); i++ {
			ids, err := t.indexManager.Query(indexedExact[i].Field, indexedExact[i].Value)
			if err != nil {
				return nil, err
			}
			recordIDs = intersectIDs(recordIDs, ids)
			if len(recordIDs) == 0 {
				return emptyResult[T](offset, limit), nil
			}
		}
	}

	if len(nonIndexed) > 0 {
		table := t
		filtered, err := t.Filter(func(record T) bool {
			if !table.matchesCondition(record, nonIndexed) {
				return false
			}
			if len(recordIDs) > 0 {
				return containsID(recordIDs, table.getRecordIDFromRecord(record))
			}
			return true
		})
		if err != nil {
			return nil, err
		}

		totalCount := len(filtered)

		if offset >= totalCount {
			return &PagedResult[T]{
				Records:    []T{},
				TotalCount: totalCount,
				HasMore:    false,
				Offset:     offset,
				Limit:      limit,
			}, nil
		}

		end := offset + limit
		if limit <= 0 || end > totalCount {
			end = totalCount
		}

		return &PagedResult[T]{
			Records:    filtered[offset:end],
			TotalCount: totalCount,
			HasMore:    end < totalCount,
			Offset:     offset,
			Limit:      limit,
		}, nil
	}

	if len(recordIDs) == 0 {
		return emptyResult[T](offset, limit), nil
	}

	t.db.lock.RLock()
	idx := t.db.indexes[t.tableID]
	t.db.lock.RUnlock()

	var results []T
	for _, id := range recordIDs {
		offsetVal, exists := idx[id]
		if !exists {
			continue
		}

		recordBytes, err := t.db.readRecordBytesAt(offsetVal, t.tableID)
		if err != nil {
			continue
		}

		if !isActiveRecord(recordBytes) {
			continue
		}

		record, err := t.db.decodeRecordWithLayout(recordBytes, t.layout)
		if err != nil {
			continue
		}

		results = append(results, *record)
	}

	totalCount := len(results)

	if offset >= totalCount {
		return &PagedResult[T]{
			Records:    []T{},
			TotalCount: totalCount,
			HasMore:    false,
			Offset:     offset,
			Limit:      limit,
		}, nil
	}

	end := offset + limit
	if limit <= 0 || end > totalCount {
		end = totalCount
	}

	return &PagedResult[T]{
		Records:    results[offset:end],
		TotalCount: totalCount,
		HasMore:    end < totalCount,
		Offset:     offset,
		Limit:      limit,
	}, nil
}

func emptyResult[T any](offset, limit int) *PagedResult[T] {
	return &PagedResult[T]{
		Records:    []T{},
		TotalCount: 0,
		HasMore:    false,
		Offset:     offset,
		Limit:      limit,
	}
}

func (t *Table[T]) getRecordIDFromRecord(record T) uint32 {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if f := v.FieldByName("ID"); f.IsValid() && f.CanUint() {
		return uint32(f.Uint())
	}
	return 0
}

func (t *Table[T]) matchesCondition(record T, conditions []*Condition) bool {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for _, cond := range conditions {
		fieldVal := getFieldByPath(v, cond.Field)
		if !fieldVal.IsValid() {
			return false
		}

		fieldValue := fieldVal
		if fieldVal.Kind() == reflect.Ptr && !fieldVal.IsNil() {
			fieldValue = fieldVal.Elem()
		}

		condValue := reflect.ValueOf(cond.Value)

		switch cond.Operator {
		case OpExact:
			if !reflect.DeepEqual(fieldValue.Interface(), condValue.Interface()) {
				return false
			}
		case OpGT:
			if compare(fieldValue, condValue) <= 0 {
				return false
			}
		case OpGTE:
			if compare(fieldValue, condValue) < 0 {
				return false
			}
		case OpLT:
			if compare(fieldValue, condValue) >= 0 {
				return false
			}
		case OpLTE:
			if compare(fieldValue, condValue) > 0 {
				return false
			}
		case OpNotEq:
			if compare(fieldValue, condValue) == 0 {
				return false
			}
		case OpBetween:
			condValue2 := reflect.ValueOf(cond.Value2)
			if compare(fieldValue, condValue) < 0 || compare(fieldValue, condValue2) > 0 {
				return false
			}
		case OpLike:
			pattern, ok := cond.Value.(string)
			if !ok || !likeMatch(fieldValue.String(), pattern) {
				return false
			}
		}
	}
	return true
}

func compare(a, b reflect.Value) int {
	aVal := a.Interface()
	bVal := b.Interface()

	switch v := aVal.(type) {
	case int:
		bInt, ok := bVal.(int)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case int8:
		bInt, ok := bVal.(int8)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case int16:
		bInt, ok := bVal.(int16)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case int32:
		bInt, ok := bVal.(int32)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case int64:
		bInt, ok := bVal.(int64)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case uint:
		bInt, ok := bVal.(uint)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case uint8:
		bInt, ok := bVal.(uint8)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case uint16:
		bInt, ok := bVal.(uint16)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case uint32:
		bInt, ok := bVal.(uint32)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case uint64:
		bInt, ok := bVal.(uint64)
		if !ok {
			return 1
		}
		if v < bInt {
			return -1
		}
		if v > bInt {
			return 1
		}
		return 0
	case float32:
		bFloat, ok := bVal.(float32)
		if !ok {
			return 1
		}
		if v < bFloat {
			return -1
		}
		if v > bFloat {
			return 1
		}
		return 0
	case float64:
		bFloat, ok := bVal.(float64)
		if !ok {
			return 1
		}
		if v < bFloat {
			return -1
		}
		if v > bFloat {
			return 1
		}
		return 0
	case string:
		bStr, ok := bVal.(string)
		if !ok {
			return 1
		}
		if v < bStr {
			return -1
		}
		if v > bStr {
			return 1
		}
		return 0
	}
	return 0
}

func likeMatch(s, pattern string) bool {
	pattern = strings.TrimSpace(pattern)

	if strings.HasPrefix(pattern, "LIKE") {
		pattern = strings.TrimSpace(pattern[4:])
	}

	hasLeadingPercent := strings.HasPrefix(pattern, "%")
	hasTrailingPercent := strings.HasSuffix(pattern, "%")

	pattern = strings.Trim(pattern, "%")

	if hasLeadingPercent && hasTrailingPercent {
		return strings.Contains(s, pattern)
	}
	if hasLeadingPercent {
		return strings.HasSuffix(s, pattern)
	}
	if hasTrailingPercent {
		return strings.HasPrefix(s, pattern)
	}
	return s == pattern
}

func intersectIDs(a, b []uint32) []uint32 {
	aSet := make(map[uint32]bool)
	for _, id := range a {
		aSet[id] = true
	}

	var result []uint32
	for _, id := range b {
		if aSet[id] {
			result = append(result, id)
		}
	}
	return result
}

func containsID(ids []uint32, target uint32) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func getFieldByPath(v reflect.Value, path string) reflect.Value {
	parts := strings.Split(path, ".")
	current := v

	for i, part := range parts {
		if !current.IsValid() {
			return reflect.Value{}
		}

		fieldIndex := -1
		t := current.Type()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
			if current.Elem().Kind() != reflect.Struct {
				return reflect.Value{}
			}
			current = current.Elem()
		}

		if current.Kind() != reflect.Struct {
			return reflect.Value{}
		}

		for j := 0; j < t.NumField(); j++ {
			if t.Field(j).Name == part {
				fieldIndex = j
				break
			}
		}

		if fieldIndex == -1 {
			return reflect.Value{}
		}

		current = current.Field(fieldIndex)

		if i < len(parts)-1 && current.Kind() == reflect.Ptr {
			if current.IsNil() {
				return reflect.Value{}
			}
			current = current.Elem()
		}
	}

	return current
}
