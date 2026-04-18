package embeddb

type PagedResult[T any] struct {
	Records    []T
	TotalCount int
	HasMore    bool
	Offset     int
	Limit      int
}

func (t *Table[T]) QueryPaged(fieldName string, value interface{}, offset, limit int) (*PagedResult[T], error) {
	all, err := t.Query(fieldName, value)
	if err != nil {
		field, fieldErr := t.findField(fieldName)
		if fieldErr != nil {
			return nil, err
		}
		comparator := func(fieldVal interface{}) bool {
			return compareValues(fieldVal, value) == 0
		}
		return t.filterPagedByField(field, comparator, offset, limit)
	}
	return paginateResults(all, offset, limit), nil
}

func (t *Table[T]) QueryRangeGreaterThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	all, err := t.QueryRangeGreaterThan(fieldName, value, inclusive)
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

func (t *Table[T]) QueryRangeLessThanPaged(fieldName string, value interface{}, inclusive bool, offset, limit int) (*PagedResult[T], error) {
	all, err := t.QueryRangeLessThan(fieldName, value, inclusive)
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

func (t *Table[T]) QueryRangeBetweenPaged(fieldName string, min, max interface{}, inclusiveMin, inclusiveMax bool, offset, limit int) (*PagedResult[T], error) {
	all, err := t.QueryRangeBetween(fieldName, min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

func (t *Table[T]) FilterPaged(fn func(T) bool, offset, limit int) (*PagedResult[T], error) {
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

		if fn(*record) {
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

	hasMore := (skipped + len(results)) < totalCount

	return &PagedResult[T]{
		Records:    results,
		TotalCount: totalCount,
		HasMore:    hasMore,
		Offset:     offset,
		Limit:      limit,
	}, nil
}

func (t *Table[T]) AllPaged(offset, limit int) (*PagedResult[T], error) {
	all, err := t.All()
	if err != nil {
		return nil, err
	}
	return paginateResults(all, offset, limit), nil
}

func paginateResults[T any](all []T, offset, limit int) *PagedResult[T] {
	if limit < 0 {
		limit = 0
	}
	if offset < 0 {
		offset = 0
	}
	total := len(all)
	if offset >= total || limit == 0 {
		return &PagedResult[T]{
			Records:    []T{},
			TotalCount: total,
			HasMore:    false,
			Offset:     offset,
			Limit:      limit,
		}
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return &PagedResult[T]{
		Records:    all[offset:end],
		TotalCount: total,
		HasMore:    end < total,
		Offset:     offset,
		Limit:      limit,
	}
}
