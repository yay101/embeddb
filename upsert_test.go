package embeddb

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"
)

type LargeDocument struct {
	ID        uint32 `db:"id,primary"`
	Title     string `db:"index"`
	Content   string
	Summary   string
	Metadata  string
	Author    string
	CreatedAt time.Time
	UpdatedAt time.Time
	Tags      []string
}

type IntPKRecord struct {
	ID   int    `db:"id,primary"`
	Name string `db:"index"`
	Data string
}

type AccountRecord struct {
	ID                       int            `db:"accountId,primary" json:"accountId"`
	VendorAccountID          string         `db:"vendorAccountId,index" json:"vendorAccountId"`
	CloudCustomerCompanyName string         `db:"cloudCustomerCompanyName,index" json:"cloudCustomerCompanyName"`
	Subscriptions            []Subscription `json:"subscriptions,omitempty"`
	ServiceDetails           ServiceDetail  `json:"serviceDetails,omitempty"`
}

type Subscription struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	Status             string          `json:"status"`
	StartDate          string          `json:"startDate"`
	EndDate            string          `json:"endDate"`
	RenewalDate        string          `json:"renewalDate"`
	LastRenewalDate    string          `json:"lastRenewalDate"`
	LicenseAutoRenewal bool            `json:"licenseAutoRenewal"`
	Count              int             `json:"count"`
	Total              int             `json:"total"`
	Trial              bool            `json:"trial"`
	Charged            string          `json:"charged"`
	Term               string          `json:"term"`
	Type               string          `json:"type"`
	NCEPlan            bool            `json:"ncePlan"`
	ScheduledChange    ScheduledChange `json:"scheduledChange"`
}

type ScheduledChange struct {
	Change   int    `json:"change"`
	Schedule string `json:"schedule"`
	CancelBy string `json:"cancelby"`
}

type ServiceDetail struct {
	ServiceID int `json:"serviceId"`
}

func TestUpsertWithIntPK(t *testing.T) {
	os.Remove("/tmp/upsert_int_pk.db")
	defer os.Remove("/tmp/upsert_int_pk.db")

	db, err := Open("/tmp/upsert_int_pk.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[IntPKRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}

	_, inserted, err := tbl.Upsert(25235, &IntPKRecord{ID: 25235, Name: "Test Account", Data: "some data"})
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if !inserted {
		t.Error("expected inserted=true for new record")
	}

	rec, err := tbl.Get(25235)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rec.ID != 25235 {
		t.Errorf("expected ID 25235, got %d", rec.ID)
	}
	if rec.Name != "Test Account" {
		t.Errorf("expected Name 'Test Account', got %s", rec.Name)
	}

	_, inserted, err = tbl.Upsert(25235, &IntPKRecord{ID: 25235, Name: "Updated Name", Data: "updated data"})
	if err != nil {
		t.Fatalf("Upsert update failed: %v", err)
	}
	if inserted {
		t.Error("expected inserted=false for existing record")
	}

	rec, err = tbl.Get(25235)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name != "Updated Name" {
		t.Errorf("expected updated name, got %s", rec.Name)
	}

	count := tbl.Count()
	if count != 1 {
		t.Errorf("expected 1 record, got %d", count)
	}
}

func TestUpsertLargeStruct(t *testing.T) {
	os.Remove("/tmp/upsert_large.db")
	defer os.Remove("/tmp/upsert_large.db")

	db, err := Open("/tmp/upsert_large.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	largeContent := makeLargeString(10000)
	largeSummary := makeLargeString(2000)

	// First insert via upsert
	id, inserted, err := tbl.Upsert(uint32(1), &LargeDocument{
		ID:        1,
		Title:     "Initial Title",
		Content:   largeContent,
		Summary:   largeSummary,
		Author:    "Alice",
		CreatedAt: time.Now(),
		Tags:      []string{"draft", "important"},
	})
	if err != nil {
		t.Fatalf("Upsert insert failed: %v", err)
	}
	if !inserted {
		t.Error("expected inserted=true for new record")
	}
	if id != 1 {
		t.Errorf("expected ID 1, got %d", id)
	}

	// Verify first insert
	doc, err := tbl.Get(1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if doc.Title != "Initial Title" {
		t.Errorf("Title mismatch: got %s", doc.Title)
	}
	if len(doc.Content) != 10000 {
		t.Errorf("Content length: got %d, want 10000", len(doc.Content))
	}
	if len(doc.Tags) != 2 {
		t.Errorf("Tags length: got %d, want 2", len(doc.Tags))
	}

	// Upsert again (should update)
	updatedContent := makeLargeString(15000)
	_, inserted, err = tbl.Upsert(uint32(1), &LargeDocument{
		ID:        1,
		Title:     "Updated Title",
		Content:   updatedContent,
		Summary:   "New summary",
		Author:    "Bob",
		UpdatedAt: time.Now(),
		Tags:      []string{"published"},
	})
	if err != nil {
		t.Fatalf("Upsert update failed: %v", err)
	}
	if inserted {
		t.Error("expected inserted=false for existing record")
	}

	// Verify update
	doc, err = tbl.Get(1)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if doc.Title != "Updated Title" {
		t.Errorf("Title after update: got %s, want Updated Title", doc.Title)
	}
	if doc.Author != "Bob" {
		t.Errorf("Author after update: got %s, want Bob", doc.Author)
	}
	if len(doc.Content) != 15000 {
		t.Errorf("Content length after update: got %d, want 15000", len(doc.Content))
	}
	if doc.Tags[0] != "published" {
		t.Errorf("Tags after update: got %v", doc.Tags)
	}
}

func TestUpsertWithIndexes(t *testing.T) {
	os.Remove("/tmp/upsert_indexed.db")
	defer os.Remove("/tmp/upsert_indexed.db")

	db, err := Open("/tmp/upsert_indexed.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert via upsert
	tbl.Upsert(uint32(1), &LargeDocument{ID: 1, Title: "Doc One", Author: "Alice"})
	tbl.Upsert(uint32(2), &LargeDocument{ID: 2, Title: "Doc Two", Author: "Bob"})

	// Query by indexed field
	results, err := tbl.Query("Title", "Doc One")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	// Upsert to update indexed field
	tbl.Upsert(uint32(1), &LargeDocument{ID: 1, Title: "Doc One Updated", Author: "Alice Updated"})

	// Query by old indexed value - should NOT find it after update
	results, err = tbl.Query("Title", "Doc One")
	if err != nil {
		t.Fatalf("Query after update failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for old title after update, got %d", len(results))
	}

	// Query by new indexed value - should find it
	results, err = tbl.Query("Title", "Doc One Updated")
	if err != nil {
		t.Fatalf("Query by new title failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result for new title, got %d", len(results))
	}
	if results[0].Author != "Alice Updated" {
		t.Errorf("Author mismatch: got %s", results[0].Author)
	}
}

func TestUpsertPersistence(t *testing.T) {
	os.Remove("/tmp/upsert_persist.db")
	defer os.Remove("/tmp/upsert_persist.db")

	{
		db, err := Open("/tmp/upsert_persist.db")
		if err != nil {
			t.Fatal(err)
		}

		tbl, err := Use[LargeDocument](db, "docs")
		if err != nil {
			t.Fatal(err)
		}

		tbl.Upsert(uint32(1), &LargeDocument{
			ID:      1,
			Title:   "Persistent Doc",
			Content: makeLargeString(5000),
		})

		db.Close()
	}

	{
		db2, err := Open("/tmp/upsert_persist.db")
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()

		tbl2, err := Use[LargeDocument](db2, "docs")
		if err != nil {
			t.Fatal(err)
		}

		doc, err := tbl2.Get(uint32(1))
		if err != nil {
			t.Fatalf("Get after reopen failed: %v", err)
		}
		if doc.Title != "Persistent Doc" {
			t.Errorf("Title mismatch: got %s", doc.Title)
		}
		if len(doc.Content) != 5000 {
			t.Errorf("Content length: got %d", len(doc.Content))
		}

		// Update via upsert after reopen
		tbl2.Upsert(uint32(1), &LargeDocument{
			ID:      1,
			Title:   "Updated Persistent",
			Content: makeLargeString(8000),
		})

		doc, err = tbl2.Get(uint32(1))
		if err != nil {
			t.Fatal(err)
		}
		if doc.Title != "Updated Persistent" {
			t.Errorf("Title after update: got %s", doc.Title)
		}
		if len(doc.Content) != 8000 {
			t.Errorf("Content length after update: got %d", len(doc.Content))
		}
	}
}

func TestUpsertMany(t *testing.T) {
	os.Remove("/tmp/upsert_many.db")
	defer os.Remove("/tmp/upsert_many.db")

	db, err := Open("/tmp/upsert_many.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert 50 records
	for i := 1; i <= 50; i++ {
		_, _, err := tbl.Upsert(uint32(i), &LargeDocument{
			ID:      uint32(i),
			Title:   "Doc " + string(rune('A'+i-1)),
			Content: makeLargeString(1000 * i),
		})
		if err != nil {
			t.Fatalf("Upsert %d failed: %v", i, err)
		}
	}

	count := tbl.Count()
	if count != 50 {
		t.Errorf("Expected 50 records, got %d", count)
	}

	// Update all via upsert
	for i := 1; i <= 50; i++ {
		_, inserted, err := tbl.Upsert(uint32(i), &LargeDocument{
			ID:      uint32(i),
			Title:   "Updated Doc " + string(rune('A'+i-1)),
			Content: makeLargeString(2000 * i),
		})
		if err != nil {
			t.Fatalf("Upsert update %d failed: %v", i, err)
		}
		if inserted {
			t.Errorf("Expected inserted=false for record %d", i)
		}
	}

	// Verify all updated
	for i := 1; i <= 50; i++ {
		doc, err := tbl.Get(uint32(i))
		if err != nil {
			t.Fatalf("Get %d failed: %v", i, err)
		}
		expectedTitle := "Updated Doc " + string(rune('A'+i-1))
		if doc.Title != expectedTitle {
			t.Errorf("Record %d: got %s, want %s", i, doc.Title, expectedTitle)
		}
	}
}

func TestDeleteWithIndexes(t *testing.T) {
	os.Remove("/tmp/delete_indexed.db")
	defer os.Remove("/tmp/delete_indexed.db")

	db, err := Open("/tmp/delete_indexed.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert records
	tbl.Insert(&LargeDocument{ID: 1, Title: "First", Author: "Alice"})
	tbl.Insert(&LargeDocument{ID: 2, Title: "Second", Author: "Bob"})
	tbl.Insert(&LargeDocument{ID: 3, Title: "Third", Author: "Charlie"})

	// Verify queries work
	results, _ := tbl.Query("Title", "First")
	if len(results) != 1 {
		t.Fatalf("Expected 1 result for First, got %d", len(results))
	}

	// Delete a record
	err = tbl.Delete(uint32(1))
	if err != nil {
		t.Fatal(err)
	}

	// Query should not find deleted record
	results, _ = tbl.Query("Title", "First")
	if len(results) != 0 {
		t.Errorf("Expected 0 results after delete, got %d", len(results))
	}

	// Other records should still be findable
	results, _ = tbl.Query("Title", "Second")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for Second, got %d", len(results))
	}
}

func TestDeleteManyWithIndexes(t *testing.T) {
	os.Remove("/tmp/delete_many_indexed.db")
	defer os.Remove("/tmp/delete_many_indexed.db")

	db, err := Open("/tmp/delete_many_indexed.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[LargeDocument](db, "docs")
	if err != nil {
		t.Fatal(err)
	}

	// Insert records
	for i := 1; i <= 10; i++ {
		tbl.Insert(&LargeDocument{ID: uint32(i), Title: string(rune('A' + i - 1)), Author: "User"})
	}

	// Delete multiple
	deleted, err := tbl.DeleteMany([]any{uint32(1), uint32(3), uint32(5)})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Errorf("Expected 3 deleted, got %d", deleted)
	}

	// Verify deleted are gone
	for _, id := range []uint32{1, 3, 5} {
		_, err := tbl.Get(id)
		if err == nil {
			t.Errorf("Expected record %d to be deleted", id)
		}
	}

	// Verify remaining
	for _, id := range []uint32{2, 4, 6, 7, 8, 9, 10} {
		_, err := tbl.Get(id)
		if err != nil {
			t.Errorf("Expected record %d to exist, got error: %v", id, err)
		}
	}

	// Verify indexes cleaned up
	results, _ := tbl.Query("Title", "A")
	if len(results) != 0 {
		t.Errorf("Expected 0 results for deleted A, got %d", len(results))
	}
	results, _ = tbl.Query("Title", "B")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for B, got %d", len(results))
	}
}

func makeLargeString(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte('a' + (i % 26))
	}
	return string(b)
}

func fetchURL(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	return buf, err
}

type CityInspection struct {
	ID                string `db:"id,primary"`
	CertificateNumber any    `db:"index"`
	BusinessName      string `db:"index"`
	Date              string
	Result            string `db:"index"`
	Sector            string
	Address           InspectionAddress
}

type InspectionAddress struct {
	City   interface{}
	Zip    interface{}
	Street interface{}
	Number interface{}
}

func TestCityInspectionsFull(t *testing.T) {
	os.Remove("/tmp/city_inspections_full.db")
	defer os.Remove("/tmp/city_inspections_full.db")

	jsonData, err := fetchURL("https://raw.githubusercontent.com/ozlerhakan/mongodb-json-files/master/datasets/city_inspections.json")
	if err != nil {
		t.Skipf("skipping: failed to fetch JSON: %v", err)
	}

	records := make([]CityInspection, 0, 100000)
	dec := json.NewDecoder(bytes.NewReader(jsonData))
	for dec.More() {
		var rec CityInspection
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("decode error at %d: %v", len(records), err)
		}
		records = append(records, rec)
	}

	total := len(records)
	if total < 1000 {
		t.Skipf("skipping: not enough records (%d)", total)
	}

	t.Logf("Fetched %d records", total)

	db, err := Open("/tmp/city_inspections_full.db")
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[CityInspection](db, "inspections")
	if err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]bool)
	duplicates := 0
	uniqueRecords := make([]CityInspection, 0, len(records))
	for i, rec := range records {
		if seen[rec.ID] {
			duplicates++
			continue
		}
		seen[rec.ID] = true
		uniqueRecords = append(uniqueRecords, rec)
		_, _, err := tbl.Upsert(rec.ID, &rec)
		if err != nil {
			t.Fatalf("upsert failed at %d: %v", i, err)
		}
	}
	t.Logf("Inserted %d unique records, %d duplicates in JSON", len(uniqueRecords), duplicates)

	db.Close()

	db2, err := Open("/tmp/city_inspections_full.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	tbl2, err := Use[CityInspection](db2, "inspections")
	if err != nil {
		t.Fatal(err)
	}

	count := tbl2.Count()
	t.Logf("After reopen: Count()=%d", count)
	if count != len(uniqueRecords) {
		t.Errorf("Count() = %d, want %d", count, len(uniqueRecords))
	}

	rand.Seed(12345)
	failed := 0
	for i := 0; i < 1000; i++ {
		randIdx := rand.Intn(len(uniqueRecords))
		original := uniqueRecords[randIdx]

		got, err := tbl2.Get(original.ID)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", original.ID, err)
			failed++
			continue
		}

		if got.BusinessName != original.BusinessName {
			t.Errorf("record %s: BusinessName mismatch: got %q, want %q",
				original.ID, got.BusinessName, original.BusinessName)
			failed++
		}
		if got.CertificateNumber != original.CertificateNumber {
			t.Errorf("record %s: CertificateNumber mismatch: got %d, want %d",
				original.ID, got.CertificateNumber, original.CertificateNumber)
			failed++
		}
		if got.Result != original.Result {
			t.Errorf("record %s: Result mismatch: got %q, want %q",
				original.ID, got.Result, original.Result)
			failed++
		}
	}

	if failed > 0 {
		t.Errorf("%d / 1000 random records failed verification", failed)
	}
}

func TestCityInspections(t *testing.T) {
	os.Remove("/tmp/city_inspections.db")
	defer os.Remove("/tmp/city_inspections.db")

	db, err := Open("/tmp/city_inspections.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[CityInspection](db, "inspections")
	if err != nil {
		t.Fatal(err)
	}

	jsonData := `[
{"_id":{"$oid":"56d61033a378eccde8a8354f"},"id":"10021-2015-ENFO","certificate_number":9278806,"business_name":"ATLIXCO DELI GROCERY INC.","date":"Feb 20 2015","result":"No Violation Issued","sector":"Cigarette Retail Dealer - 127","address":{"city":"RIDGEWOOD","zip":11385,"street":"MENAHAN ST","number":1712}},
{"_id":{"$oid":"56d61033a378eccde8a83550"},"id":"10057-2015-ENFO","certificate_number":6007104,"business_name":"LD BUSINESS SOLUTIONS","date":"Feb 25 2015","result":"Violation Issued","sector":"Tax Preparers - 891","address":{"city":"NEW YORK","zip":10030,"street":"FREDERICK DOUGLASS BLVD","number":2655}},
{"_id":{"$oid":"56d61033a378eccde8a83551"},"id":"10084-2015-ENFO","certificate_number":9278914,"business_name":"MICHAEL GOMEZ RANGHALL","date":"Feb 10 2015","result":"No Violation Issued","sector":"Locksmith - 062","address":{"city":"QUEENS VLG","zip":11427,"street":"214TH ST","number":8823}},
{"_id":{"$oid":"56d61033a378eccde8a83552"},"id":"1012-2015-CMPL","certificate_number":5346909,"business_name":"A&C CHIMNEY CORP.","date":"Apr 22 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"QUEENS VLG","zip":11428,"street":"210TH ST","number":9440}},
{"_id":{"$oid":"56d61033a378eccde8a83553"},"id":"10127-2015-CMPL","certificate_number":5381180,"business_name":"ERIC CONSTRUCTION AND DECORATING INC.","date":"Sep  8 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"STATEN ISLAND","zip":10304,"street":"TODT HILL RD","number":1233}},
{"_id":{"$oid":"56d61033a378eccde8a83554"},"id":"10172-2015-CMPL","certificate_number":9304489,"business_name":"UNNAMED HOT DOG VENDOR LICENSE NUMBER TA01158","date":"Aug 21 2015","result":"No Violation Issued","sector":"Mobile Food Vendor - 881","address":{"city":"","zip":"","street":"","number":0}},
{"_id":{"$oid":"56d61033a378eccde8a83555"},"id":"102-2015-UNIT","certificate_number":10003479,"business_name":"SOUTH BRONX AUTOMOTIVE CORP","date":"May 28 2015","result":"Pass","sector":"Tow Truck Company - 124","address":{"city":"","zip":"","street":"","number":0}},
{"_id":{"$oid":"56d61033a378eccde8a83556"},"id":"10268-2015-CMPL","certificate_number":9304816,"business_name":"UNNAMED HOT DOG VENDOR NO LICENSE NUMBER PROVIDED","date":"Aug 19 2015","result":"No Violation Issued","sector":"Mobile Food Vendor - 881","address":{"city":"","zip":"","street":"","number":0}},
{"_id":{"$oid":"56d61033a378eccde8a83557"},"id":"10284-2015-ENFO","certificate_number":9287088,"business_name":"VYACHESLAV KANDZHANOV","date":"Feb 25 2015","result":"No Violation Issued","sector":"Misc Non-Food Retail - 817","address":{"city":"NEW YORK","zip":10030,"street":"TONNELE AVE","number":70}},
{"_id":{"$oid":"56d61033a378eccde8a83558"},"id":"10312-2015-ENFO","certificate_number":9287090,"business_name":"GRICEYDA M VILLAR","date":"Feb 25 2015","result":"No Violation Issued","sector":"Salons And Barbershop - 841","address":{"city":"NEW YORK","zip":10030,"street":"FREDRCK D BLVD","number":2645}},
{"_id":{"$oid":"56d61033a378eccde8a83559"},"id":"10302-2015-ENFO","certificate_number":9287089,"business_name":"NYC CANDY STORE SHOP CORP","date":"Feb 25 2015","result":"No Violation Issued","sector":"Cigarette Retail Dealer - 127","address":{"city":"NEW YORK","zip":10030,"street":"FREDRCK D BLVD","number":2653}},
{"_id":{"$oid":"56d61033a378eccde8a8355a"},"id":"10290-2015-CMPL","certificate_number":9305498,"business_name":"AYAD YOUSSEF","date":"Jul 23 2015","result":"No Violation Issued","sector":"Mobile Food Vendor - 881","address":{"city":"JERSEY CITY","zip":7306,"street":"TONNELE AVE","number":70}},
{"_id":{"$oid":"56d61033a378eccde8a8355b"},"id":"10318-2015-ENFO","certificate_number":9287092,"business_name":"BISHWANATH BISWAS","date":"Feb 25 2015","result":"No Violation Issued","sector":"Mobile Food Vendor - 881","address":{"city":"ELMHURST","zip":11373,"street":"75TH ST","number":4157}},
{"_id":{"$oid":"56d61033a378eccde8a8355c"},"id":"1033-2015-CMPL","certificate_number":5347333,"business_name":"A&H FURNITURE","date":"Apr 29 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"S RICHMOND HL","zip":11419,"street":"101ST AVE","number":11420}},
{"_id":{"$oid":"56d61033a378eccde8a8355d"},"id":"10351-2015-CMPL","certificate_number":5381196,"business_name":"NEW FINEST BUILDERS INC","date":"Dec  1 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"BROOKLYN","zip":11234,"street":"E 55TH ST","number":1320}},
{"_id":{"$oid":"56d61033a378eccde8a8355e"},"id":"10391-2015-ENFO","certificate_number":3019415,"business_name":"WILFREDO DELIVERY SERVICE INC","date":"Feb 26 2015","result":"Fail","sector":"Fuel Oil Dealer - 814","address":{"city":"WADING RIVER","zip":11792,"street":"WADING RIVER MANOR RD","number":1607}},
{"_id":{"$oid":"56d61033a378eccde8a8355f"},"id":"10423-2015-CMPL","certificate_number":9304139,"business_name":"LISANDRO CABRE","date":"Jul  1 2015","result":"Violation Issued","sector":"Mobile Food Vendor - 881","address":{"city":"BROOKLYN","zip":11223,"street":"KINGS HWY","number":2822}},
{"_id":{"$oid":"56d61033a378eccde8a83560"},"id":"10458-2015-ENFO","certificate_number":9287087,"business_name":"HAPPY GROCERY","date":"Feb 25 2015","result":"No Violation Issued","sector":"Misc Non-Food Retail - 817","address":{"city":"NEW YORK","zip":10030,"street":"FREDRCK D BLVD","number":2650}},
{"_id":{"$oid":"56d61033a378eccde8a83561"},"id":"10530-2015-ENFO","certificate_number":10592661,"business_name":"JAMES GONSALVES","date":"Mar  4 2015","result":"No Violation Issued","sector":"Mobile Food Vendor - 881","address":{"city":"BROOKLYN","zip":11211,"street":"LORIMER ST","number":244}},
{"_id":{"$oid":"56d61033a378eccde8a83562"},"id":"10579-2015-ENFO","certificate_number":9287091,"business_name":"WILFREDO DELIVERY SERVICE INC","date":"Feb 26 2015","result":"Fail","sector":"Fuel Oil Dealer - 814","address":{"city":"BROOKLYN","zip":11249,"street":"LORIMER ST","number":244}},
{"_id":{"$oid":"56d61033a378eccde8a83563"},"id":"10611-2015-CMPL","certificate_number":10916289,"business_name":"ALEX CONSTRUCTION & DESIGN INC","date":"Jan  5 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"JAMAICA","zip":11432,"street":"HILLSIDE AVE","number":17576}},
{"_id":{"$oid":"56d61033a378eccde8a83564"},"id":"10617-2015-ENFO","certificate_number":10593208,"business_name":"CHINATOWN GROCERY INC","date":"Mar  9 2015","result":"No Violation Issued","sector":"Misc Non-Food Retail - 817","address":{"city":"NEW YORK","zip":10002,"street":"CANAL ST","number":70}},
{"_id":{"$oid":"56d61033a378eccde8a83565"},"id":"10628-2015-CMPL","certificate_number":12077082,"business_name":"GEORGES HOME IMPROVEMENT CORP","date":"Jan 21 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"BROOKLYN","zip":11204,"street":"BAY PKWY","number":6804}},
{"_id":{"$oid":"56d61033a378eccde8a83566"},"id":"10647-2015-ENFO","certificate_number":10593206,"business_name":"NEW YORK GROCERY CORP","date":"Mar  9 2015","result":"No Violation Issued","sector":"Misc Non-Food Retail - 817","address":{"city":"NEW YORK","zip":10002,"street":"CANAL ST","number":70}},
{"_id":{"$oid":"56d61033a378eccde8a83567"},"id":"10662-2015-CMPL","certificate_number":10916290,"business_name":"APEX BUILDERS NY INC","date":"Jan  5 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"JAMAICA","zip":11432,"street":"HILLSIDE AVE","number":17576}},
{"_id":{"$oid":"56d61033a378eccde8a83568"},"id":"10674-2015-ENFO","certificate_number":10593205,"business_name":"BEST GROCERY INC","date":"Mar  9 2015","result":"No Violation Issued","sector":"Misc Non-Food Retail - 817","address":{"city":"NEW YORK","zip":10002,"street":"CANAL ST","number":70}},
{"_id":{"$oid":"56d61033a378eccde8a83569"},"id":"10702-2015-CMPL","certificate_number":9278807,"business_name":"ATLIXCO DELI GROCERY INC.","date":"Feb 20 2015","result":"No Violation Issued","sector":"Cigarette Retail Dealer - 127","address":{"city":"RIDGEWOOD","zip":11385,"street":"MENAHAN ST","number":1712}},
{"_id":{"$oid":"56d61033a378eccde8a8356a"},"id":"10716-2015-ENFO","certificate_number":10593204,"business_name":"LUCKY GROCERY CORP","date":"Mar  9 2015","result":"No Violation Issued","sector":"Misc Non-Food Retail - 817","address":{"city":"NEW YORK","zip":10002,"street":"CANAL ST","number":70}},
{"_id":{"$oid":"56d61033a378eccde8a8356b"},"id":"10758-2015-CMPL","certificate_number":12077084,"business_name":"S BROTHERS CONSTRUCTION CORP","date":"Jan 21 2015","result":"Violation Issued","sector":"Home Improvement Contractor - 100","address":{"city":"BROOKLYN","zip":11204,"street":"BAY PKWY","number":6804}},
{"_id":{"$oid":"56d61033a378eccde8a8356c"},"id":"10805-2015-ENFO","certificate_number":10593203,"business_name":"GOLDEN DRAGON CORP","date":"Mar  9 2015","result":"No Violation Issued","sector":"Misc Non-Food Retail - 817","address":{"city":"NEW YORK","zip":10002,"street":"CANAL ST","number":70}}
]`

	var records []CityInspection
	if err := json.Unmarshal([]byte(jsonData), &records); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	inserted := 0
	for _, rec := range records {
		_, err := tbl.Insert(&rec)
		if err != nil {
			t.Fatalf("insert failed for %s: %v", rec.ID, err)
		}
		inserted++
	}

	count := tbl.Count()
	if count != inserted {
		t.Errorf("Count() = %d, want %d", count, inserted)
	}

	all, err := tbl.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != inserted {
		t.Errorf("All() returned %d, want %d", len(all), inserted)
	}

	t.Logf("Inserted %d records, Count()=%d, All()=%d", inserted, count, len(all))
}

func TestAccountRecordsDebug(t *testing.T) {
	os.Remove("/tmp/account_debug.db")
	defer os.Remove("/tmp/account_debug.db")

	db, err := Open("/tmp/account_debug.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AccountRecord](db, "accounts")
	if err != nil {
		t.Fatal(err)
	}

	rec := AccountRecord{
		ID:                       12345,
		VendorAccountID:          "test-vendor-123",
		CloudCustomerCompanyName: "Test Company",
	}

	_, _, err = tbl.Upsert(12345, &rec)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	count := tbl.Count()
	t.Logf("Count() = %d", count)

	all, err := tbl.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	t.Logf("All() returned %d records", len(all))

	if count != len(all) {
		t.Errorf("Count() = %d, All() = %d", count, len(all))
	}
}

func TestAccountRecordsFromFile(t *testing.T) {
	os.Remove("/tmp/account_records.db")
	defer os.Remove("/tmp/account_records.db")

	db, err := Open("/tmp/account_records.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[AccountRecord](db, "accounts")
	if err != nil {
		t.Fatal(err)
	}

	jsonData, err := os.ReadFile("datatest.json")
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	var records []AccountRecord
	if err := json.Unmarshal(jsonData, &records); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	t.Logf("Loaded %d records from file", len(records))

	seen := make(map[int]bool)
	unique := 0
	for _, rec := range records {
		if !seen[rec.ID] {
			seen[rec.ID] = true
			unique++
			_, _, err := tbl.Upsert(rec.ID, &rec)
			if err != nil {
				t.Fatalf("Upsert failed for accountId %d: %v", rec.ID, err)
			}
		}
	}
	t.Logf("Upserted %d unique records", unique)

	count := tbl.Count()
	t.Logf("Count() = %d", count)
	if count != unique {
		t.Errorf("Count() = %d, want %d", count, unique)
	}

	all, err := tbl.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	t.Logf("All() returned %d records", len(all))
	if len(all) != unique {
		t.Errorf("All() returned %d, want %d", len(all), unique)
	}
}
