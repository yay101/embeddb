package embeddb

import (
	"os"
	"testing"
	"time"
)

type AllSliceTypesRecord struct {
	ID         uint32 `db:"id,primary"`
	Name       string `db:"index"`
	Int8Slc    []int8
	Int16Slc   []int16
	Int32Slc   []int32
	Int64Slc   []int64
	UintSlc    []uint
	Uint16Slc  []uint16
	Uint32Slc  []uint32
	Uint64Slc  []uint64
	Float32Slc []float32
	Float64Slc []float64
	BoolSlc    []bool
}

func TestAllSliceTypesRoundTrip(t *testing.T) {
	os.Remove("/tmp/test_all_slice_types.db")
	defer os.Remove("/tmp/test_all_slice_types.db")

	db, err := Open("/tmp/test_all_slice_types.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AllSliceTypesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	rec := &AllSliceTypesRecord{
		Name:       "slices",
		Int8Slc:    []int8{-1, 0, 127},
		Int16Slc:   []int16{-32768, 0, 32767},
		Int32Slc:   []int32{-2147483648, 0, 2147483647},
		Int64Slc:   []int64{-9223372036854775808, 0, 9223372036854775807},
		UintSlc:    []uint{0, 42, 999},
		Uint16Slc:  []uint16{0, 1000, 65535},
		Uint32Slc:  []uint32{0, 100000, 4294967295},
		Uint64Slc:  []uint64{0, 1, 18446744073709551615},
		Float32Slc: []float32{1.1, 2.2, 3.3},
		Float64Slc: []float64{1.23456789, 2.0, -3.5},
		BoolSlc:    []bool{true, false, true},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "slices" {
		t.Errorf("Name: got %q, want %q", got.Name, "slices")
	}

	checkSliceInt8 := func(name string, got, want []int8) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceInt16 := func(name string, got, want []int16) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceInt32 := func(name string, got, want []int32) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceInt64 := func(name string, got, want []int64) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceUint := func(name string, got, want []uint) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceUint16 := func(name string, got, want []uint16) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceUint32 := func(name string, got, want []uint32) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceUint64 := func(name string, got, want []uint64) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %d, want %d", name, i, got[i], want[i])
			}
		}
	}
	checkSliceFloat32 := func(name string, got, want []float32) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %f, want %f", name, i, got[i], want[i])
			}
		}
	}
	checkSliceFloat64 := func(name string, got, want []float64) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %f, want %f", name, i, got[i], want[i])
			}
		}
	}
	checkSliceBool := func(name string, got, want []bool) {
		if len(got) != len(want) {
			t.Errorf("%s: got len %d, want len %d", name, len(got), len(want))
			return
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s[%d]: got %v, want %v", name, i, got[i], want[i])
			}
		}
	}

	checkSliceInt8("Int8Slc", got.Int8Slc, rec.Int8Slc)
	checkSliceInt16("Int16Slc", got.Int16Slc, rec.Int16Slc)
	checkSliceInt32("Int32Slc", got.Int32Slc, rec.Int32Slc)
	checkSliceInt64("Int64Slc", got.Int64Slc, rec.Int64Slc)
	checkSliceUint("UintSlc", got.UintSlc, rec.UintSlc)
	checkSliceUint16("Uint16Slc", got.Uint16Slc, rec.Uint16Slc)
	checkSliceUint32("Uint32Slc", got.Uint32Slc, rec.Uint32Slc)
	checkSliceUint64("Uint64Slc", got.Uint64Slc, rec.Uint64Slc)
	checkSliceFloat32("Float32Slc", got.Float32Slc, rec.Float32Slc)
	checkSliceFloat64("Float64Slc", got.Float64Slc, rec.Float64Slc)
	checkSliceBool("BoolSlc", got.BoolSlc, rec.BoolSlc)
}

func TestAllSliceTypesEmptySlices(t *testing.T) {
	os.Remove("/tmp/test_empty_slices.db")
	defer os.Remove("/tmp/test_empty_slices.db")

	db, err := Open("/tmp/test_empty_slices.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AllSliceTypesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	rec := &AllSliceTypesRecord{
		Name:       "empty",
		Int8Slc:    []int8{},
		Int16Slc:   []int16{},
		Int32Slc:   []int32{},
		Int64Slc:   []int64{},
		UintSlc:    []uint{},
		Uint16Slc:  []uint16{},
		Uint32Slc:  []uint32{},
		Uint64Slc:  []uint64{},
		Float32Slc: []float32{},
		Float64Slc: []float64{},
		BoolSlc:    []bool{},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "empty" {
		t.Errorf("Name: got %q, want %q", got.Name, "empty")
	}
	if len(got.Int8Slc) != 0 {
		t.Errorf("Int8Slc: got len %d, want 0", len(got.Int8Slc))
	}
	if len(got.Int16Slc) != 0 {
		t.Errorf("Int16Slc: got len %d, want 0", len(got.Int16Slc))
	}
	if len(got.Int32Slc) != 0 {
		t.Errorf("Int32Slc: got len %d, want 0", len(got.Int32Slc))
	}
	if len(got.Int64Slc) != 0 {
		t.Errorf("Int64Slc: got len %d, want 0", len(got.Int64Slc))
	}
	if len(got.UintSlc) != 0 {
		t.Errorf("UintSlc: got len %d, want 0", len(got.UintSlc))
	}
	if len(got.Uint16Slc) != 0 {
		t.Errorf("Uint16Slc: got len %d, want 0", len(got.Uint16Slc))
	}
	if len(got.Uint32Slc) != 0 {
		t.Errorf("Uint32Slc: got len %d, want 0", len(got.Uint32Slc))
	}
	if len(got.Uint64Slc) != 0 {
		t.Errorf("Uint64Slc: got len %d, want 0", len(got.Uint64Slc))
	}
	if len(got.Float32Slc) != 0 {
		t.Errorf("Float32Slc: got len %d, want 0", len(got.Float32Slc))
	}
	if len(got.Float64Slc) != 0 {
		t.Errorf("Float64Slc: got len %d, want 0", len(got.Float64Slc))
	}
	if len(got.BoolSlc) != 0 {
		t.Errorf("BoolSlc: got len %d, want 0", len(got.BoolSlc))
	}
}

func TestAllSliceTypesPersistence(t *testing.T) {
	os.Remove("/tmp/test_slice_persist.db")

	db, err := Open("/tmp/test_slice_persist.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[AllSliceTypesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	rec := &AllSliceTypesRecord{
		Name:       "persist",
		Int8Slc:    []int8{-10, 20},
		Int16Slc:   []int16{-1000, 2000},
		Int32Slc:   []int32{-100000, 200000},
		Int64Slc:   []int64{-5000000000, 5000000000},
		UintSlc:    []uint{10, 20},
		Uint16Slc:  []uint16{100, 200},
		Uint32Slc:  []uint32{1000, 2000},
		Uint64Slc:  []uint64{10000, 20000},
		Float32Slc: []float32{1.5, 2.5},
		Float64Slc: []float64{3.14, 2.72},
		BoolSlc:    []bool{true, false},
	}

	_, err = tbl.Insert(rec)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open("/tmp/test_slice_persist.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	tbl2, err := Use[AllSliceTypesRecord](db2, "test")
	if err != nil {
		t.Fatal(err)
	}

	got, err := tbl2.Get(uint32(1))
	if err != nil {
		t.Fatalf("Get after persist: %v", err)
	}

	if got.Name != "persist" {
		t.Errorf("Name: got %q, want %q", got.Name, "persist")
	}
	if len(got.Int8Slc) != 2 || got.Int8Slc[0] != -10 || got.Int8Slc[1] != 20 {
		t.Errorf("Int8Slc: got %v", got.Int8Slc)
	}
	if len(got.Int16Slc) != 2 || got.Int16Slc[0] != -1000 || got.Int16Slc[1] != 2000 {
		t.Errorf("Int16Slc: got %v", got.Int16Slc)
	}
	if len(got.Int32Slc) != 2 || got.Int32Slc[0] != -100000 || got.Int32Slc[1] != 200000 {
		t.Errorf("Int32Slc: got %v", got.Int32Slc)
	}
	if len(got.Int64Slc) != 2 || got.Int64Slc[0] != -5000000000 || got.Int64Slc[1] != 5000000000 {
		t.Errorf("Int64Slc: got %v", got.Int64Slc)
	}
	if len(got.UintSlc) != 2 || got.UintSlc[0] != 10 || got.UintSlc[1] != 20 {
		t.Errorf("UintSlc: got %v", got.UintSlc)
	}
	if len(got.Uint16Slc) != 2 || got.Uint16Slc[0] != 100 || got.Uint16Slc[1] != 200 {
		t.Errorf("Uint16Slc: got %v", got.Uint16Slc)
	}
	if len(got.Uint32Slc) != 2 || got.Uint32Slc[0] != 1000 || got.Uint32Slc[1] != 2000 {
		t.Errorf("Uint32Slc: got %v", got.Uint32Slc)
	}
	if len(got.Uint64Slc) != 2 || got.Uint64Slc[0] != 10000 || got.Uint64Slc[1] != 20000 {
		t.Errorf("Uint64Slc: got %v", got.Uint64Slc)
	}
	if len(got.Float32Slc) != 2 || got.Float32Slc[0] != 1.5 || got.Float32Slc[1] != 2.5 {
		t.Errorf("Float32Slc: got %v", got.Float32Slc)
	}
	if len(got.Float64Slc) != 2 || got.Float64Slc[0] != 3.14 || got.Float64Slc[1] != 2.72 {
		t.Errorf("Float64Slc: got %v", got.Float64Slc)
	}
	if len(got.BoolSlc) != 2 || got.BoolSlc[0] != true || got.BoolSlc[1] != false {
		t.Errorf("BoolSlc: got %v", got.BoolSlc)
	}

	os.Remove("/tmp/test_slice_persist.db")
}

type StructWithAllSliceTypes struct {
	Scores    []float64
	Flags     []bool
	Intent8s  []int8
	Intent16s []int16
	Intent32s []int32
	Intent64s []int64
	Uints     []uint
	Uint16s   []uint16
	Uint32s   []uint32
	Uint64s   []uint64
	Flt32s    []float32
}

type RecordWithStructSlices struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Inner StructWithAllSliceTypes
}

func TestSliceTypesInsideStruct(t *testing.T) {
	os.Remove("/tmp/test_slices_in_struct.db")
	defer os.Remove("/tmp/test_slices_in_struct.db")

	db, err := Open("/tmp/test_slices_in_struct.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[RecordWithStructSlices](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	rec := &RecordWithStructSlices{
		Name: "nested_slices",
		Inner: StructWithAllSliceTypes{
			Scores:    []float64{99.5, 87.3, 72.1},
			Flags:     []bool{true, false, true, true},
			Intent8s:  []int8{-1, 0, 1},
			Intent16s: []int16{-100, 0, 100},
			Intent32s: []int32{-1000, 0, 1000},
			Intent64s: []int64{-99999, 0, 99999},
			Uints:     []uint{1, 2, 3},
			Uint16s:   []uint16{10, 20},
			Uint32s:   []uint32{100, 200},
			Uint64s:   []uint64{1000, 2000},
			Flt32s:    []float32{1.1, 2.2},
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

	if got.Name != "nested_slices" {
		t.Errorf("Name: got %q, want %q", got.Name, "nested_slices")
	}
	if len(got.Inner.Scores) != 3 || got.Inner.Scores[0] != 99.5 || got.Inner.Scores[2] != 72.1 {
		t.Errorf("Inner.Scores: got %v", got.Inner.Scores)
	}
	if len(got.Inner.Flags) != 4 || got.Inner.Flags[0] != true || got.Inner.Flags[1] != false {
		t.Errorf("Inner.Flags: got %v", got.Inner.Flags)
	}
	if len(got.Inner.Intent8s) != 3 || got.Inner.Intent8s[0] != -1 || got.Inner.Intent8s[2] != 1 {
		t.Errorf("Inner.Intent8s: got %v", got.Inner.Intent8s)
	}
	if len(got.Inner.Intent16s) != 3 || got.Inner.Intent16s[0] != -100 {
		t.Errorf("Inner.Intent16s: got %v", got.Inner.Intent16s)
	}
	if len(got.Inner.Intent32s) != 3 || got.Inner.Intent32s[0] != -1000 {
		t.Errorf("Inner.Intent32s: got %v", got.Inner.Intent32s)
	}
	if len(got.Inner.Intent64s) != 3 || got.Inner.Intent64s[0] != -99999 {
		t.Errorf("Inner.Intent64s: got %v", got.Inner.Intent64s)
	}
	if len(got.Inner.Uints) != 3 || got.Inner.Uints[1] != 2 {
		t.Errorf("Inner.Uints: got %v", got.Inner.Uints)
	}
	if len(got.Inner.Uint16s) != 2 || got.Inner.Uint16s[0] != 10 {
		t.Errorf("Inner.Uint16s: got %v", got.Inner.Uint16s)
	}
	if len(got.Inner.Uint32s) != 2 || got.Inner.Uint32s[0] != 100 {
		t.Errorf("Inner.Uint32s: got %v", got.Inner.Uint32s)
	}
	if len(got.Inner.Uint64s) != 2 || got.Inner.Uint64s[0] != 1000 {
		t.Errorf("Inner.Uint64s: got %v", got.Inner.Uint64s)
	}
	if len(got.Inner.Flt32s) != 2 || got.Inner.Flt32s[0] != 1.1 {
		t.Errorf("Inner.Flt32s: got %v", got.Inner.Flt32s)
	}
}

type Measurement struct {
	Timestamp time.Time
	Value     float64
	Sensor    string
}

type Reading struct {
	Timestamp time.Time
	Reading   float64
	Labels    []string
	Scores    []int
	Vitals    []float64
	Active    []bool
}

type RecordWithStructSliceAllTypes struct {
	ID       uint32 `db:"id,primary"`
	Name     string
	Readings []Reading
}

func TestStructSlicesWithAllInnerTypes(t *testing.T) {
	os.Remove("/tmp/test_struct_slices_all_types.db")
	defer os.Remove("/tmp/test_struct_slices_all_types.db")

	db, err := Open("/tmp/test_struct_slices_all_types.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[RecordWithStructSliceAllTypes](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	rec := &RecordWithStructSliceAllTypes{
		Name: "multi_type_structs",
		Readings: []Reading{
			{
				Timestamp: ts1,
				Reading:   98.6,
				Labels:    []string{"a", "b"},
				Scores:    []int{10, 20},
				Vitals:    []float64{1.1, 2.2},
				Active:    []bool{true, false},
			},
			{
				Timestamp: ts2,
				Reading:   72.5,
				Labels:    []string{"c"},
				Scores:    []int{30},
				Vitals:    []float64{3.3},
				Active:    []bool{true},
			},
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

	if got.Name != "multi_type_structs" {
		t.Errorf("Name: got %q", got.Name)
	}
	if len(got.Readings) != 2 {
		t.Fatalf("Readings: got len %d, want 2", len(got.Readings))
	}
	if !got.Readings[0].Timestamp.Equal(ts1) {
		t.Errorf("Readings[0].Timestamp: got %v, want %v", got.Readings[0].Timestamp, ts1)
	}
	if got.Readings[0].Reading != 98.6 {
		t.Errorf("Readings[0].Reading: got %f, want 98.6", got.Readings[0].Reading)
	}
	if len(got.Readings[0].Labels) != 2 || got.Readings[0].Labels[0] != "a" || got.Readings[0].Labels[1] != "b" {
		t.Errorf("Readings[0].Labels: got %v", got.Readings[0].Labels)
	}
	if len(got.Readings[0].Scores) != 2 || got.Readings[0].Scores[0] != 10 || got.Readings[0].Scores[1] != 20 {
		t.Errorf("Readings[0].Scores: got %v", got.Readings[0].Scores)
	}
	if len(got.Readings[0].Vitals) != 2 || got.Readings[0].Vitals[0] != 1.1 || got.Readings[0].Vitals[1] != 2.2 {
		t.Errorf("Readings[0].Vitals: got %v", got.Readings[0].Vitals)
	}
	if len(got.Readings[0].Active) != 2 || got.Readings[0].Active[0] != true || got.Readings[0].Active[1] != false {
		t.Errorf("Readings[0].Active: got %v", got.Readings[0].Active)
	}
	if !got.Readings[1].Timestamp.Equal(ts2) {
		t.Errorf("Readings[1].Timestamp: got %v, want %v", got.Readings[1].Timestamp, ts2)
	}
	if len(got.Readings[1].Labels) != 1 || got.Readings[1].Labels[0] != "c" {
		t.Errorf("Readings[1].Labels: got %v", got.Readings[1].Labels)
	}
	if len(got.Readings[1].Scores) != 1 || got.Readings[1].Scores[0] != 30 {
		t.Errorf("Readings[1].Scores: got %v", got.Readings[1].Scores)
	}
	if len(got.Readings[1].Vitals) != 1 || got.Readings[1].Vitals[0] != 3.3 {
		t.Errorf("Readings[1].Vitals: got %v", got.Readings[1].Vitals)
	}
	if len(got.Readings[1].Active) != 1 || got.Readings[1].Active[0] != true {
		t.Errorf("Readings[1].Active: got %v", got.Readings[1].Active)
	}
}

func TestAllSliceTypesUpdate(t *testing.T) {
	os.Remove("/tmp/test_slice_types_update.db")
	defer os.Remove("/tmp/test_slice_types_update.db")

	db, err := Open("/tmp/test_slice_types_update.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AllSliceTypesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	rec := &AllSliceTypesRecord{
		Name:       "original",
		Int8Slc:    []int8{1},
		Int16Slc:   []int16{10},
		Float64Slc: []float64{1.0},
		BoolSlc:    []bool{true},
	}

	id, err := tbl.Insert(rec)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	updated := &AllSliceTypesRecord{
		ID:         id,
		Name:       "updated",
		Int8Slc:    []int8{-5, 0, 5},
		Int16Slc:   []int16{-50, 0, 50},
		Int32Slc:   []int32{-500, 0, 500},
		Int64Slc:   []int64{-5000, 0, 5000},
		UintSlc:    []uint{100, 200},
		Uint16Slc:  []uint16{1000},
		Uint32Slc:  []uint32{50000},
		Uint64Slc:  []uint64{999999},
		Float32Slc: []float32{1.5, 2.5},
		Float64Slc: []float64{3.14, 2.72},
		BoolSlc:    []bool{false, true, false},
	}

	err = tbl.Update(id, updated)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, err := tbl.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "updated" {
		t.Errorf("Name: got %q, want %q", got.Name, "updated")
	}
	if len(got.Int8Slc) != 3 || got.Int8Slc[0] != -5 || got.Int8Slc[2] != 5 {
		t.Errorf("Int8Slc: got %v", got.Int8Slc)
	}
	if len(got.BoolSlc) != 3 || got.BoolSlc[0] != false || got.BoolSlc[1] != true {
		t.Errorf("BoolSlc: got %v", got.BoolSlc)
	}
	if len(got.Float64Slc) != 2 || got.Float64Slc[0] != 3.14 {
		t.Errorf("Float64Slc: got %v", got.Float64Slc)
	}
	if len(got.Uint64Slc) != 1 || got.Uint64Slc[0] != 999999 {
		t.Errorf("Uint64Slc: got %v", got.Uint64Slc)
	}
}

func TestFilterWithSliceFields(t *testing.T) {
	os.Remove("/tmp/test_filter_slices.db")
	defer os.Remove("/tmp/test_filter_slices.db")

	db, err := Open("/tmp/test_filter_slices.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AllSliceTypesRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&AllSliceTypesRecord{Name: "alpha", Int8Slc: []int8{1, 2}, Float64Slc: []float64{10.0}, BoolSlc: []bool{true}})
	tbl.Insert(&AllSliceTypesRecord{Name: "beta", Int16Slc: []int16{-5, 5}, Float64Slc: []float64{20.0, 30.0}, BoolSlc: []bool{false, true}})
	tbl.Insert(&AllSliceTypesRecord{Name: "gamma", Int64Slc: []int64{100}, UintSlc: []uint{50, 60}, Float32Slc: []float32{1.5}})

	results, err := tbl.Filter(func(r AllSliceTypesRecord) bool {
		return r.Name == "beta"
	})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	r := results[0]
	if len(r.Int16Slc) != 2 || r.Int16Slc[0] != -5 || r.Int16Slc[1] != 5 {
		t.Errorf("Int16Slc: got %v", r.Int16Slc)
	}
	if len(r.Float64Slc) != 2 || r.Float64Slc[0] != 20.0 || r.Float64Slc[1] != 30.0 {
		t.Errorf("Float64Slc: got %v", r.Float64Slc)
	}
	if len(r.BoolSlc) != 2 || r.BoolSlc[0] != false || r.BoolSlc[1] != true {
		t.Errorf("BoolSlc: got %v", r.BoolSlc)
	}

	results, err = tbl.Filter(func(r AllSliceTypesRecord) bool {
		return len(r.UintSlc) > 0
	})
	if err != nil {
		t.Fatalf("Filter by UintSlc: %v", err)
	}
	if len(results) != 1 || results[0].Name != "gamma" {
		t.Errorf("Filter by UintSlc: expected gamma, got %d results", len(results))
	}
	if len(results[0].UintSlc) != 2 || results[0].UintSlc[0] != 50 {
		t.Errorf("UintSlc: got %v", results[0].UintSlc)
	}
}

type AllScalarQueryRecord struct {
	ID  uint32  `db:"id,primary"`
	I8  int8    `db:"index"`
	I16 int16   `db:"index"`
	I32 int32   `db:"index"`
	I64 int64   `db:"index"`
	U   uint    `db:"index"`
	U8  uint8   `db:"index"`
	U16 uint16  `db:"index"`
	U32 uint32  `db:"index"`
	U64 uint64  `db:"index"`
	F32 float32 `db:"index"`
	F64 float64 `db:"index"`
	B   bool    `db:"index"`
	Str string  `db:"index"`
}

func TestAllScalarTypesQueryRange(t *testing.T) {
	os.Remove("/tmp/test_scalar_query_range.db")
	defer os.Remove("/tmp/test_scalar_query_range.db")

	db, err := Open("/tmp/test_scalar_query_range.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AllScalarQueryRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	tbl.Insert(&AllScalarQueryRecord{I8: 1, I16: 100, I32: 1000, I64: 10000, U: 10, U8: 1, U16: 100, U32: 1000, U64: 10000, F32: 1.5, F64: 2.5, B: true, Str: "alpha"})
	tbl.Insert(&AllScalarQueryRecord{I8: 5, I16: 500, I32: 5000, I64: 50000, U: 50, U8: 5, U16: 500, U32: 5000, U64: 50000, F32: 5.5, F64: 10.5, B: false, Str: "beta"})
	tbl.Insert(&AllScalarQueryRecord{I8: 10, I16: 1000, I32: 10000, I64: 100000, U: 100, U8: 10, U16: 1000, U32: 10000, U64: 100000, F32: 10.0, F64: 100.0, B: true, Str: "gamma"})

	_ = tbl

	results, err := tbl.QueryRangeGreaterThan("I8", int8(3), true)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan I8: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("I8 > 3 inclusive: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeLessThan("I16", int16(500), false)
	if err != nil {
		t.Fatalf("QueryRangeLessThan I16: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("I16 < 500 exclusive: expected 1, got %d", len(results))
	}

	results, err = tbl.QueryRangeBetween("I32", int32(1000), int32(10000), true, true)
	if err != nil {
		t.Fatalf("QueryRangeBetween I32: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("I32 between 1000-10000 inclusive: expected 3, got %d", len(results))
	}

	results, err = tbl.QueryRangeGreaterThan("I64", int64(50000), false)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan I64: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("I64 > 50000 exclusive: expected 1, got %d", len(results))
	}

	results, err = tbl.QueryRangeGreaterThan("U", uint(50), true)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan U: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("U >= 50: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeLessThan("U16", uint16(500), true)
	if err != nil {
		t.Fatalf("QueryRangeLessThan U16: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("U16 <= 500: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeBetween("U32", uint32(1000), uint32(10000), true, true)
	if err != nil {
		t.Fatalf("QueryRangeBetween U32: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("U32 between 1000-10000 inclusive: expected 3, got %d", len(results))
	}

	results, err = tbl.QueryRangeGreaterThan("U64", uint64(50000), false)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan U64: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("U64 > 50000 exclusive: expected 1, got %d", len(results))
	}

	results, err = tbl.QueryRangeBetween("F32", float32(1.5), float32(10.0), true, false)
	if err != nil {
		t.Fatalf("QueryRangeBetween F32: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("F32 between 1.5 inclusive and 10.0 exclusive: expected 2, got %d", len(results))
	}

	results, err = tbl.QueryRangeGreaterThan("F64", float64(10.0), true)
	if err != nil {
		t.Fatalf("QueryRangeGreaterThan F64: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("F64 >= 10.0: expected 2, got %d", len(results))
	}

	results, err = tbl.Query("B", true)
	if err != nil {
		t.Fatalf("Query B: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Query B=true: expected 2, got %d", len(results))
	}

	results, err = tbl.Query("U8", uint8(5))
	if err != nil {
		t.Fatalf("Query U8: %v", err)
	}
	if len(results) != 1 || results[0].U8 != 5 {
		t.Errorf("Query U8=5: expected 1, got %d", len(results))
	}

	results, err = tbl.Filter(func(r AllScalarQueryRecord) bool {
		return r.I64 > 50000
	})
	if err != nil {
		t.Fatalf("Filter I64: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Filter I64 > 50000: expected 1, got %d", len(results))
	}
}
