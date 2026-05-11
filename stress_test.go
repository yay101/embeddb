package embeddb

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

type StressAddress struct {
	Street string `db:"index"`
	City   string `db:"index"`
	State  string
	Zip    string
}

type StressReading struct {
	Label     string
	Value     float64
	Timestamp time.Time
}

type StressEvent struct {
	ID          uint32    `db:"id,primary"`
	Title       string    `db:"index"`
	Category    string    `db:"index"`
	Priority    int       `db:"index"`
	CreatedAt   time.Time `db:"index"`
	Location    StressAddress
	Tags        []string
	SensorData  []StressReading
	Description string
}

func TestStress10M(t *testing.T) {
	const (
		numTables      = 10
		perTable       = 100000
		deletePerRound = 10000
		rounds         = 11
	)
	totalOps := numTables*perTable + numTables*deletePerRound*rounds
	t.Logf("Stress test: %d tables x %d records, %d rounds of delete %d + reopen + verify = ~%d total ops",
		numTables, perTable, rounds, deletePerRound, totalOps)

	rand.Seed(time.Now().UnixNano())
	dbPath := "/home/dave/tmp/stress_10m.db"
	os.Remove(dbPath)

	db, err := Open(dbPath, OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	tableNames := []string{
		"events_0", "events_1", "events_2", "events_3", "events_4",
		"events_5", "events_6", "events_7", "events_8", "events_9",
	}
	tables := make([]*Table[StressEvent], numTables)
	for i := 0; i < numTables; i++ {
		tbl, err := Use[StressEvent](db, tableNames[i])
		if err != nil {
			t.Fatalf("use table %s: %v", tableNames[i], err)
		}
		tables[i] = tbl
	}

	categories := []string{"alert", "warning", "info", "critical", "debug"}
	priorities := []int{1, 2, 3, 4, 5}
	states := []string{"CA", "NY", "TX", "FL", "WA", "IL", "CO", "OR", "GA", "MA"}
	cities := []string{"San Francisco", "New York", "Austin", "Miami", "Seattle", "Chicago", "Denver", "Portland", "Atlanta", "Boston"}

	start := time.Now()

	type trackedRecord struct {
		id       uint32
		category string
		priority int
		title    string
		state    string
	}

	allRecords := make([][]trackedRecord, numTables)

	t.Log("Phase 1: Inserting 100k records per table...")
	var wg sync.WaitGroup
	for i := 0; i < numTables; i++ {
		wg.Add(1)
		go func(tblIdx int) {
			defer wg.Done()
			records := make([]trackedRecord, 0, perTable)
			for j := 0; j < perTable; j++ {
				cat := categories[j%len(categories)]
				pri := priorities[j%len(priorities)]
				title := fmt.Sprintf("Event-%d-%d", tblIdx, j)
				state := states[j%len(states)]
				city := cities[j%len(cities)]
				ts := time.Date(2024, 1, 1, 0, 0, 0, j%1000000000, time.UTC).Add(time.Duration(j) * time.Second)

				ev := &StressEvent{
					Title:     title,
					Category:  cat,
					Priority:  pri,
					CreatedAt: ts,
					Location: StressAddress{
						Street: fmt.Sprintf("%d Main St", j),
						City:   city,
						State:  state,
						Zip:    fmt.Sprintf("%05d", j%100000),
					},
					Tags: []string{fmt.Sprintf("tag%d", j%5), fmt.Sprintf("tag%d", j%3)},
					SensorData: []StressReading{
						{Label: "temp", Value: float64(j%100) + 0.5, Timestamp: ts},
						{Label: "humidity", Value: float64(j%80) + 10.0, Timestamp: ts},
					},
					Description: fmt.Sprintf("Description for event %d in table %d", j, tblIdx),
				}

				id, err := tables[tblIdx].Insert(ev)
				if err != nil {
					t.Errorf("insert table=%d record=%d: %v", tblIdx, j, err)
					return
				}
				records = append(records, trackedRecord{id: id, category: cat, priority: pri, title: title, state: state})
			}
			allRecords[tblIdx] = records
		}(i)
	}
	wg.Wait()

	insertDur := time.Since(start)
	t.Logf("Phase 1 done: inserted %d records in %v (%.0f rec/sec)",
		numTables*perTable, insertDur, float64(numTables*perTable)/insertDur.Seconds())

	for i := 0; i < numTables; i++ {
		count := tables[i].Count()
		if count != perTable {
			t.Errorf("table %s: expected %d records after insert, got %d", tableNames[i], perTable, count)
		}
	}

	t.Log("Phase 2: Query verification after insert...")
	for i := 0; i < numTables; i++ {
		results, err := tables[i].Query("Category", "critical")
		if err != nil {
			t.Errorf("query category=critical table=%d: %v", i, err)
			continue
		}
		expectedPerTable := perTable / len(categories)
		if len(results) < expectedPerTable/2 {
			t.Errorf("table %s: expected ~%d critical events, got %d", tableNames[i], expectedPerTable, len(results))
		}
		for _, r := range results {
			if r.Category != "critical" {
				t.Errorf("table %s: Query returned wrong category: %s", tableNames[i], r.Category)
				break
			}
		}

		rangeResults, err := tables[i].QueryRangeBetween("Priority", 2, 4, true, true)
		if err != nil {
			t.Errorf("range query table=%d: %v", i, err)
			continue
		}
		for _, r := range rangeResults {
			if r.Priority < 2 || r.Priority > 4 {
				t.Errorf("table %s: range query priority %d outside 2-4", tableNames[i], r.Priority)
				break
			}
		}

		timeResults, err := tables[i].QueryRangeGreaterThan("CreatedAt", time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), true)
		if err != nil {
			t.Errorf("time range query table=%d: %v", i, err)
			continue
		}
		for _, r := range timeResults {
			if r.CreatedAt.Before(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)) {
				t.Errorf("table %s: time range returned event before cutoff", tableNames[i])
				break
			}
		}
	}
	t.Log("Phase 2 done: queries verified")

	for round := 0; round < rounds; round++ {
		t.Logf("Round %d/%d: delete %d per table, close, reopen, verify", round+1, rounds, deletePerRound)

		for i := 0; i < numTables; i++ {
			delIDs := make([]any, 0, deletePerRound)
			startDel := round * deletePerRound
			endDel := startDel + deletePerRound
			if endDel > perTable {
				endDel = perTable
			}
			if startDel >= perTable {
				continue
			}
			for j := startDel; j < endDel; j++ {
				delIDs = append(delIDs, allRecords[i][j].id)
			}
			deleted, err := tables[i].DeleteMany(delIDs)
			if err != nil {
				t.Errorf("delete many table=%d round=%d: %v", i, round, err)
			}
			if deleted != endDel-startDel {
				t.Errorf("table %s: expected %d deletions, got %d", tableNames[i], endDel-startDel, deleted)
			}
		}

		db.Close()
		db, err = Open(dbPath, OpenOptions{AutoIndex: true})
		if err != nil {
			t.Fatalf("reopen db round %d: %v", round+1, err)
		}
		tables = make([]*Table[StressEvent], numTables)
		for i := 0; i < numTables; i++ {
			tbl, err := Use[StressEvent](db, tableNames[i])
			if err != nil {
				t.Fatalf("use table %s round %d: %v", tableNames[i], round+1, err)
			}
			tables[i] = tbl
		}

		deletedSoFar := (round + 1) * deletePerRound
		if deletedSoFar > perTable {
			deletedSoFar = perTable
		}
		expectedCount := perTable - deletedSoFar
		for i := 0; i < numTables; i++ {
			count := tables[i].Count()
			if count != expectedCount {
				t.Errorf("round %d table %s: expected %d records, got %d", round+1, tableNames[i], expectedCount, count)
			}
		}

		for i := 0; i < numTables; i++ {
			results, err := tables[i].Query("Category", "alert")
			if err != nil {
				t.Errorf("round %d query table=%d: %v", round+1, i, err)
				continue
			}
			for _, r := range results {
				if r.Category != "alert" {
					t.Errorf("round %d table %s: wrong category: %s", round+1, tableNames[i], r.Category)
					break
				}
				if r.Location.City == "" {
					t.Errorf("round %d table %s: empty city in nested struct", round+1, tableNames[i])
					break
				}
				if len(r.Tags) < 1 {
					t.Errorf("round %d table %s: empty tags slice", round+1, tableNames[i])
					break
				}
				if len(r.SensorData) < 1 {
					t.Errorf("round %d table %s: empty sensor data", round+1, tableNames[i])
					break
				}
			}

			rangeResults, err := tables[i].QueryGreaterOrEqual("Priority", 3)
			if err != nil {
				t.Errorf("round %d range query table=%d: %v", round+1, i, err)
				continue
			}
			for _, r := range rangeResults {
				if r.Priority < 3 {
					t.Errorf("round %d table %s: priority %d < 3", round+1, tableNames[i], r.Priority)
					break
				}
			}

			timeResults, err := tables[i].QueryLessOrEqual("CreatedAt", time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC))
			if err != nil {
				t.Errorf("round %d time query table=%d: %v", round+1, i, err)
				continue
			}
			for _, r := range timeResults {
				if r.CreatedAt.After(time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)) {
					t.Errorf("round %d table %s: time %v > cutoff", round+1, tableNames[i], r.CreatedAt)
					break
				}
			}
		}

		for i := 0; i < numTables; i++ {
			checkIdx := (round + 5) * deletePerRound
			if checkIdx < perTable && checkIdx >= deletedSoFar {
				rec, err := tables[i].Get(allRecords[i][checkIdx].id)
				if err != nil {
					t.Errorf("round %d get table=%d id=%d: %v", round+1, i, allRecords[i][checkIdx].id, err)
				} else if rec.Title != allRecords[i][checkIdx].title {
					t.Errorf("round %d table %s: expected title %q, got %q", round+1, tableNames[i], allRecords[i][checkIdx].title, rec.Title)
				} else if rec.Location.State != allRecords[i][checkIdx].state {
					t.Errorf("round %d table %s: expected state %s, got %s", round+1, tableNames[i], allRecords[i][checkIdx].state, rec.Location.State)
				} else if len(rec.SensorData) != 2 {
					t.Errorf("round %d table %s: expected 2 sensor readings, got %d", round+1, tableNames[i], len(rec.SensorData))
				}
			}
		}
	}

	totalDur := time.Since(start)
	t.Logf("Stress test complete in %v (%d total operations)", totalDur, totalOps)

	db.Close()
	os.Remove(dbPath)
}
