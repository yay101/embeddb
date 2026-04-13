package embeddb

import (
	"os"
	"testing"
	"time"
)

// ─── All Supported Primitive Types ─────────────────────────────────────────────

type AllTypesRecord struct {
	ID    uint32  `db:"id,primary"`
	Name  string  `db:"index"`
	Age   int     `db:"index"`
	Score float64 `db:"index"`
	Flag  bool
	Big   int64
	Small int8
	Count uint16
}

func TestAllTypesRoundTrip(t *testing.T) {
	os.Remove("/tmp/test_all_types.db")
	defer os.Remove("/tmp/test_all_types.db")

	db, err := Open("/tmp/test_all_types.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AllTypesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	rec := &AllTypesRecord{
		Name:  "Alice",
		Age:   30,
		Score: 95.5,
		Flag:  true,
		Big:   -123456789,
		Small: -1,
		Count: 500,
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "Alice" {
		t.Errorf("Name: got %q, want %q", got.Name, "Alice")
	}
	if got.Age != 30 {
		t.Errorf("Age: got %d, want %d", got.Age, 30)
	}
	if got.Score != 95.5 {
		t.Errorf("Score: got %f, want %f", got.Score, 95.5)
	}
	if got.Flag != true {
		t.Errorf("Flag: got %v, want %v", got.Flag, true)
	}
	if got.Big != -123456789 {
		t.Errorf("Big: got %d, want %d", got.Big, -123456789)
	}
	if got.Small != -1 {
		t.Errorf("Small: got %d, want %d", got.Small, -1)
	}
	if got.Count != 500 {
		t.Errorf("Count: got %d, want %d", got.Count, 500)
	}
}

func TestAllTypesPersistence(t *testing.T) {
	os.Remove("/tmp/test_all_types_persist.db")

	db, err := Open("/tmp/test_all_types_persist.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[AllTypesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	rec := &AllTypesRecord{
		Name:  "Bob",
		Age:   42,
		Score: 88.25,
		Flag:  false,
		Big:   9876543210,
		Small: 127,
		Count: 999,
	}

	_, err = tbl.Insert(rec)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open("/tmp/test_all_types_persist.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	tbl2, err := Use[AllTypesRecord](db2, "test")
	if err != nil {
		t.Fatal(err)
	}

	got, err := tbl2.Get(uint32(1))
	if err != nil {
		t.Fatalf("Get after persist failed: %v", err)
	}

	if got.Name != "Bob" || got.Age != 42 || got.Score != 88.25 || got.Big != 9876543210 || got.Small != 127 || got.Count != 999 {
		t.Errorf("Persistence mismatch: got Name=%s Age=%d Score=%f Big=%d Small=%d Count=%d",
			got.Name, got.Age, got.Score, got.Big, got.Small, got.Count)
	}
	if got.Flag != false {
		t.Errorf("Flag after persist: got %v, want false", got.Flag)
	}

	os.Remove("/tmp/test_all_types_persist.db")
}

// ─── Time Field Queries ────────────────────────────────────────────────────────

type TimestampRecord struct {
	ID        uint32    `db:"id,primary"`
	Name      string    `db:"index"`
	CreatedAt time.Time `db:"index"`
}

func TestTimeFieldQuery(t *testing.T) {
	os.Remove("/tmp/test_time_query.db")
	defer os.Remove("/tmp/test_time_query.db")

	db, err := Open("/tmp/test_time_query.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TimestampRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 3, 10, 14, 30, 0, 500000000, time.UTC)
	ts3 := time.Date(2024, 6, 1, 8, 0, 0, 0, time.UTC)

	tbl.Insert(&TimestampRecord{Name: "alpha", CreatedAt: ts1})
	tbl.Insert(&TimestampRecord{Name: "beta", CreatedAt: ts2})
	tbl.Insert(&TimestampRecord{Name: "gamma", CreatedAt: ts3})

	results, err := tbl.Query("CreatedAt", ts2)
	if err != nil {
		t.Fatalf("Query by time: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Query exact time: expected 1, got %d", len(results))
	}
	if len(results) > 0 && !results[0].CreatedAt.Equal(ts2) {
		t.Errorf("Query exact time: got %v, want %v", results[0].CreatedAt, ts2)
	}
}

func TestTimeFieldRangeQuery(t *testing.T) {
	os.Remove("/tmp/test_time_range.db")
	defer os.Remove("/tmp/test_time_range.db")

	db, err := Open("/tmp/test_time_range.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TimestampRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 3, 10, 14, 30, 0, 0, time.UTC)
	ts3 := time.Date(2024, 6, 1, 8, 0, 0, 0, time.UTC)

	tbl.Insert(&TimestampRecord{Name: "alpha", CreatedAt: ts1})
	tbl.Insert(&TimestampRecord{Name: "beta", CreatedAt: ts2})
	tbl.Insert(&TimestampRecord{Name: "gamma", CreatedAt: ts3})

	results, err := tbl.QueryRangeGreaterThan("CreatedAt", ts1, false)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("GreaterThan ts1 exclusive: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeGreaterThan("CreatedAt", ts1, true)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan inclusive: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("GreaterThan ts1 inclusive: expected 3, got %d", len(results))
	}

	results, err = tbl.QueryRangeLessThan("CreatedAt", ts3, false)
	if err != nil {
		t.Fatalf("QueryRangeLessThan: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("LessThan ts3 exclusive: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeBetween("CreatedAt", ts1, ts3, true, true)
	if err != nil {
		t.Fatalf("QueryRangeBetween: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Between ts1 and ts3 inclusive: expected 3, got %d", len(results))
	}

	results, err = tbl.QueryRangeBetween("CreatedAt", ts1, ts3, false, false)
	if err != nil {
		t.Fatalf("QueryRangeBetween exclusive: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Between ts1 and ts3 exclusive: expected 1, got %d", len(results))
	}
}

func TestTimeFieldFilter(t *testing.T) {
	os.Remove("/tmp/test_time_filter.db")
	defer os.Remove("/tmp/test_time_filter.db")

	db, err := Open("/tmp/test_time_filter.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TimestampRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tbl.Insert(&TimestampRecord{Name: "past", CreatedAt: ts1})
	tbl.Insert(&TimestampRecord{Name: "mid", CreatedAt: ts2})
	tbl.Insert(&TimestampRecord{Name: "future", CreatedAt: ts3})

	midpoint := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	results, err := tbl.Filter(func(r TimestampRecord) bool {
		return r.CreatedAt.Before(midpoint)
	})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Filter before midpoint: expected 2, got %d", len(results))
	}
}

func TestTimeFieldWithoutIndex(t *testing.T) {
	os.Remove("/tmp/test_time_no_index.db")
	defer os.Remove("/tmp/test_time_no_index.db")

	db, err := Open("/tmp/test_time_no_index.db", OpenOptions{AutoIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TimestampRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.CreateIndex("CreatedAt")
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	ts1 := time.Date(2024, 3, 1, 0, 0, 0, 123456789, time.UTC)
	ts2 := time.Date(2024, 3, 1, 0, 0, 0, 987654321, time.UTC)

	tbl.Insert(&TimestampRecord{Name: "a", CreatedAt: ts1})
	tbl.Insert(&TimestampRecord{Name: "b", CreatedAt: ts2})

	results, err := tbl.QueryRangeGreaterThan("CreatedAt", ts1, false)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan without auto-index: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result after ts1, got %d", len(results))
	}
	if len(results) > 0 && !results[0].CreatedAt.Equal(ts2) {
		t.Errorf("Expected ts2, got %v", results[0].CreatedAt)
	}
}

// ─── Nested Struct Persistence ─────────────────────────────────────────────────

type AddressWithTime struct {
	Street  string
	City    string `db:"index"`
	MovedIn time.Time
}

type PersonWithTimeAddress struct {
	ID      uint32 `db:"id,primary"`
	Name    string `db:"index"`
	Address AddressWithTime
}

func TestNestedStructWithTime(t *testing.T) {
	os.Remove("/tmp/test_nested_time.db")
	defer os.Remove("/tmp/test_nested_time.db")

	db, err := Open("/tmp/test_nested_time.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[PersonWithTimeAddress](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	movedIn := time.Date(2023, 7, 15, 9, 30, 0, 500000000, time.UTC)

	rec := &PersonWithTimeAddress{
		Name: "Alice",
		Address: AddressWithTime{
			Street:  "123 Main St",
			City:    "Portland",
			MovedIn: movedIn,
		},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "Alice" {
		t.Errorf("Name: got %q, want %q", got.Name, "Alice")
	}
	if got.Address.Street != "123 Main St" {
		t.Errorf("Street: got %q, want %q", got.Address.Street, "123 Main St")
	}
	if got.Address.City != "Portland" {
		t.Errorf("City: got %q, want %q", got.Address.City, "Portland")
	}
	if !got.Address.MovedIn.Equal(movedIn) {
		t.Errorf("MovedIn: got %v, want %v (nano diff: %d vs %d)",
			got.Address.MovedIn, movedIn, got.Address.MovedIn.Nanosecond(), movedIn.Nanosecond())
	}
}

func TestNestedStructQueryByField(t *testing.T) {
	os.Remove("/tmp/test_nested_query.db")
	defer os.Remove("/tmp/test_nested_query.db")

	db, err := Open("/tmp/test_nested_query.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[PersonWithTimeAddress](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&PersonWithTimeAddress{
		Name:    "Alice",
		Address: AddressWithTime{Street: "1st Ave", City: "NYC", MovedIn: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
	})
	tbl.Insert(&PersonWithTimeAddress{
		Name:    "Bob",
		Address: AddressWithTime{Street: "2nd Ave", City: "LA", MovedIn: time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)},
	})
	tbl.Insert(&PersonWithTimeAddress{
		Name:    "Charlie",
		Address: AddressWithTime{Street: "3rd Ave", City: "NYC", MovedIn: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	})

	results, err := tbl.Query("Address.City", "NYC")
	if err != nil {
		t.Fatalf("Query nested field: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Query NYC: expected 2, got %d", len(results))
	}
}

// ─── Nested Struct Range Queries ───────────────────────────────────────────────

type NestedRangeRecord struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Inner struct {
		Score int `db:"index"`
		Grade string
	}
}

func TestNestedStructRangeQuery(t *testing.T) {
	os.Remove("/tmp/test_nested_range.db")
	defer os.Remove("/tmp/test_nested_range.db")

	db, err := Open("/tmp/test_nested_range.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[NestedRangeRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&NestedRangeRecord{Name: "Alice", Inner: struct {
		Score int `db:"index"`
		Grade string
	}{Score: 85, Grade: "B"}})
	tbl.Insert(&NestedRangeRecord{Name: "Bob", Inner: struct {
		Score int `db:"index"`
		Grade string
	}{Score: 92, Grade: "A"}})
	tbl.Insert(&NestedRangeRecord{Name: "Charlie", Inner: struct {
		Score int `db:"index"`
		Grade string
	}{Score: 78, Grade: "C"}})

	results, err := tbl.QueryRangeGreaterThan("Inner.Score", 80, true)
	if err != nil {
		t.Fatalf("GreaterThan: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Score > 80 (inclusive): expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeLessThan("Inner.Score", 90, true)
	if err != nil {
		t.Fatalf("LessThan: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Score <= 90: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeBetween("Inner.Score", 75, 90, true, true)
	if err != nil {
		t.Fatalf("Between: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Score 75-90 (inclusive): expected 2, got %d", len(results))
	}
}

// ─── Struct with Multiple Nested Structs ───────────────────────────────────────

type Coordinates struct {
	Lat float64
	Lon float64
}

type EventRecord struct {
	ID        uint32    `db:"id,primary"`
	Title     string    `db:"index"`
	CreatedAt time.Time `db:"index"`
	Location  Coordinates
}

func TestMultipleStructTypesRoundTrip(t *testing.T) {
	os.Remove("/tmp/test_multi_struct.db")
	defer os.Remove("/tmp/test_multi_struct.db")

	db, err := Open("/tmp/test_multi_struct.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[EventRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2024, 8, 20, 15, 45, 30, 123456789, time.UTC)
	rec := &EventRecord{
		Title:     "Conference",
		CreatedAt: ts,
		Location:  Coordinates{Lat: 45.515, Lon: -122.678},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Title != "Conference" {
		t.Errorf("Title: got %q, want %q", got.Title, "Conference")
	}
	if !got.CreatedAt.Equal(ts) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, ts)
	}
	if got.Location.Lat != 45.515 {
		t.Errorf("Lat: got %f, want %f", got.Location.Lat, 45.515)
	}
	if got.Location.Lon != -122.678 {
		t.Errorf("Lon: got %f, want %f", got.Location.Lon, -122.678)
	}
}

func TestEventRecordTimeRangeQuery(t *testing.T) {
	os.Remove("/tmp/test_event_range.db")
	defer os.Remove("/tmp/test_event_range.db")

	db, err := Open("/tmp/test_event_range.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[EventRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	ts3 := time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)

	tbl.Insert(&EventRecord{Title: "Winter", CreatedAt: ts1, Location: Coordinates{Lat: 40.0, Lon: -74.0}})
	tbl.Insert(&EventRecord{Title: "Summer", CreatedAt: ts2, Location: Coordinates{Lat: 45.0, Lon: -122.0}})
	tbl.Insert(&EventRecord{Title: "Holiday", CreatedAt: ts3, Location: Coordinates{Lat: 51.0, Lon: -0.1}})

	results, err := tbl.QueryRangeBetween("CreatedAt", ts1, ts3, true, true)
	if err != nil {
		t.Fatalf("Between: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Between all dates: expected 3, got %d", len(results))
	}

	results, err = tbl.QueryRangeGreaterThan("CreatedAt", ts2, false)
	if err != nil {
		t.Fatalf("GreaterThan: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("After Summer: expected 1, got %d", len(results))
	}

	results, err = tbl.Query("CreatedAt", ts2)
	if err != nil {
		t.Fatalf("Query exact time: %v", err)
	}
	if len(results) != 1 || results[0].Title != "Summer" {
		t.Errorf("Query exact ts2: expected 1 'Summer', got %d results", len(results))
	}
}

// ─── Time Field Update ─────────────────────────────────────────────────────────

func TestTimeFieldUpdateWithNanoseconds(t *testing.T) {
	os.Remove("/tmp/test_time_update_nano.db")
	defer os.Remove("/tmp/test_time_update_nano.db")

	db, err := Open("/tmp/test_time_update_nano.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TimestampRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 1, 10, 0, 0, 111222333, time.UTC)
	ts2 := time.Date(2024, 6, 15, 14, 30, 0, 999888777, time.UTC)

	id, err := tbl.Insert(&TimestampRecord{Name: "test", CreatedAt: ts1})
	if err != nil {
		t.Fatal(err)
	}

	err = tbl.Update(id, &TimestampRecord{ID: id, Name: "updated", CreatedAt: ts2})
	if err != nil {
		t.Fatal(err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(ts2) {
		t.Errorf("After update: got %v (nano=%d), want %v (nano=%d)",
			got.CreatedAt, got.CreatedAt.Nanosecond(), ts2, ts2.Nanosecond())
	}
}

// ─── Embedded Struct Query ─────────────────────────────────────────────────────

type EmbeddedAddress struct {
	City    string `db:"index"`
	Country string
}

type PersonWithEmbedded struct {
	ID   uint32 `db:"id,primary"`
	Name string `db:"index"`
	EmbeddedAddress
}

func TestEmbeddedStructQueryAndFilter(t *testing.T) {
	os.Remove("/tmp/test_embedded_query.db")
	defer os.Remove("/tmp/test_embedded_query.db")

	db, err := Open("/tmp/test_embedded_query.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[PersonWithEmbedded](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&PersonWithEmbedded{Name: "Alice", EmbeddedAddress: EmbeddedAddress{City: "NYC", Country: "USA"}})
	tbl.Insert(&PersonWithEmbedded{Name: "Bob", EmbeddedAddress: EmbeddedAddress{City: "LA", Country: "USA"}})
	tbl.Insert(&PersonWithEmbedded{Name: "Charlie", EmbeddedAddress: EmbeddedAddress{City: "London", Country: "UK"}})

	results, err := tbl.Query("City", "NYC")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Name != "Alice" {
		t.Errorf("Query City=NYC: expected Alice, got %d results", len(results))
	}

	results, err = tbl.Filter(func(p PersonWithEmbedded) bool {
		return p.Country == "USA"
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("Filter Country=USA: expected 2, got %d", len(results))
	}
}

// ─── Bool Field Queries ────────────────────────────────────────────────────────

type BoolRecord struct {
	ID      uint32 `db:"id,primary"`
	Name    string `db:"index"`
	Active  bool   `db:"index"`
	Balance float64
}

func TestBoolFieldRoundTripAndQuery(t *testing.T) {
	os.Remove("/tmp/test_bool.db")
	defer os.Remove("/tmp/test_bool.db")

	db, err := Open("/tmp/test_bool.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[BoolRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&BoolRecord{Name: "Alice", Active: true, Balance: 100.50})
	tbl.Insert(&BoolRecord{Name: "Bob", Active: false, Balance: 200.75})
	tbl.Insert(&BoolRecord{Name: "Charlie", Active: true, Balance: 50.25})

	results, err := tbl.Query("Active", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("Query Active=true: expected 2, got %d", len(results))
	}

	results, err = tbl.Query("Active", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("Query Active=false: expected 1, got %d", len(results))
	}
}

// ─── Float Field Range Queries ─────────────────────────────────────────────────

func TestFloatRangeQueries(t *testing.T) {
	os.Remove("/tmp/test_float_range.db")
	defer os.Remove("/tmp/test_float_range.db")

	db, err := Open("/tmp/test_float_range.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[BoolRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&BoolRecord{Name: "A", Active: true, Balance: 10.5})
	tbl.Insert(&BoolRecord{Name: "B", Active: false, Balance: 50.0})
	tbl.Insert(&BoolRecord{Name: "C", Active: true, Balance: 99.9})

	results, err := tbl.QueryRangeGreaterThan("Balance", 40.0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("Balance >= 40: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeLessThan("Balance", 50.0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("Balance < 50: expected 1, got %d", len(results))
	}
}

// ─── Slice of Structs with Time ────────────────────────────────────────────────

type TimeBlock struct {
	Start time.Time
	End   time.Time
	Label string
}

type ScheduleRecord struct {
	ID      uint32 `db:"id,primary"`
	Name    string `db:"index"`
	Blocks  []TimeBlock
	Created time.Time
}

func TestSliceOfStructsWithTime(t *testing.T) {
	os.Remove("/tmp/test_slice_time.db")
	defer os.Remove("/tmp/test_slice_time.db")

	db, err := Open("/tmp/test_slice_time.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[ScheduleRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	start1 := time.Date(2024, 3, 1, 9, 0, 0, 123456789, time.UTC)
	end1 := time.Date(2024, 3, 1, 17, 0, 0, 987654321, time.UTC)
	created := time.Date(2024, 2, 28, 10, 0, 0, 0, time.UTC)

	rec := &ScheduleRecord{
		Name:    "workday",
		Created: created,
		Blocks: []TimeBlock{
			{Start: start1, End: end1, Label: "morning"},
			{Start: time.Date(2024, 3, 1, 18, 0, 0, 0, time.UTC),
				End:   time.Date(2024, 3, 1, 20, 0, 0, 0, time.UTC),
				Label: "evening"},
		},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.Created.Equal(created) {
		t.Errorf("Created: got %v, want %v", got.Created, created)
	}
	if !got.Blocks[0].Start.Equal(start1) {
		t.Errorf("Blocks[0].Start: got %v, want %v", got.Blocks[0].Start, start1)
	}
	if !got.Blocks[0].End.Equal(end1) {
		t.Errorf("Blocks[0].End: got %v, want %v", got.Blocks[0].End, end1)
	}
}
