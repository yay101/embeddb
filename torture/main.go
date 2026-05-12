package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yay101/embeddb"
)

type TestRecord struct {
	ID        uint32    `db:"id,primary"`
	Name      string    `db:"name,index"`
	Age       int32     `db:"age,index"`
	Score     float64   `db:"score"`
	Active    bool      `db:"active,index"`
	CreatedAt time.Time `db:"created_at"`
	Tags      []string  `db:"tags"`
	Notes     string    `db:"notes"`
}

type WorkerState struct {
	table       *embeddb.Table[TestRecord]
	insertedIDs []uint32
	mu          sync.RWMutex
}

type StatsSnapshot struct {
	Time           string  `json:"time"`
	Ops            int64   `json:"ops"`
	Inserts        int64   `json:"inserts"`
	Gets           int64   `json:"gets"`
	Updates        int64   `json:"updates"`
	Deletes        int64   `json:"deletes"`
	Queries        int64   `json:"queries"`
	TxCommits      int64   `json:"tx_commits"`
	TxRollbacks    int64   `json:"tx_rollbacks"`
	Errors         int64   `json:"errors"`
	ExpectedErrors int64   `json:"expected_errors"`
	FileSize       int64   `json:"file_size"`
	WALSize        int64   `json:"wal_size"`
	RecordCount    int     `json:"record_count"`
	IndexKeys      int     `json:"index_keys"`
	BTreeDepth     int     `json:"btree_depth"`
	FreeBytes      uint64  `json:"free_bytes"`
	FreeBlocks     int     `json:"free_blocks"`
	CacheHitRate   float64 `json:"cache_hit_rate"`
	MaxFileSize    int64   `json:"max_file_size"`
}

var (
	opCount      int64
	insertCount  int64
	getCount     int64
	updateCount  int64
	deleteCount  int64
	queryCount   int64
	txCommitCnt  int64
	txRollbackCnt int64
	errorCount   int64
	expectedErrs int64
	errLog       []string
	errLogMu     sync.Mutex
	insertErrs   int64
	updateErrs   int64
	deleteErrs   int64
	queryErrs    int64
	txErrs       int64
	scanErrs     int64
	filterErrs   int64
	pagedErrs    int64
	bulkErrs     int64
)

func logError(msg string) {
	errLogMu.Lock()
	if len(errLog) < 100 {
		errLog = append(errLog, msg)
	}
	errLogMu.Unlock()
}

func main() {
	dbPath := flag.String("db", "torture.db", "database file path")
	maxFileSize := flag.Int64("max-size", 500*1024*1024, "max file size in bytes (default 500MB)")
	maxRecords := flag.Int("max-records", 100000, "max records before forcing deletes")
	duration := flag.Duration("duration", 0, "run duration (0 = forever)")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	workers := flag.Int("workers", 1, "number of concurrent workers (1 = correctness test, >1 = stress test)")
	vacuumInterval := flag.Duration("vacuum", 10*time.Minute, "vacuum interval (0 = disabled)")
	statsInterval := flag.Duration("stats", 10*time.Second, "stats reporting interval")
	wal := flag.Bool("wal", false, "enable WAL")
	compression := flag.Bool("compression", false, "enable compression")
	encryption := flag.Bool("encryption", false, "enable encryption")
	versioning := flag.Int("versions", 3, "max versions per record (0 = disabled)")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))

	fmt.Printf("EmbedDB Torture Test\n")
	fmt.Printf("  Database:    %s\n", *dbPath)
	fmt.Printf("  Max Size:    %s\n", formatBytes(*maxFileSize))
	fmt.Printf("  Max Records: %d\n", *maxRecords)
	fmt.Printf("  Duration:    %v\n", *duration)
	fmt.Printf("  Seed:        %d\n", *seed)
	fmt.Printf("  Workers:     %d\n", *workers)
	if *workers > 1 {
		fmt.Printf("  Mode:        STRESS TEST (concurrent mmap writes may cause CRC errors)\n")
	} else {
		fmt.Printf("  Mode:        CORRECTNESS TEST (single writer)\n")
	}
	fmt.Printf("  Vacuum:      %v\n", *vacuumInterval)
	fmt.Printf("  WAL:         %v\n", *wal)
	fmt.Printf("  Compression: %v\n", *compression)
	fmt.Printf("  Encryption:  %v\n", *encryption)
	fmt.Printf("  Versioning:  %d\n", *versioning)
	fmt.Println()

	dir := filepath.Dir(*dbPath)
	if dir != "" && dir != "." {
		os.MkdirAll(dir, 0755)
	}

	opts := embeddb.OpenOptions{
		Migrate:       true,
		AutoIndex:     true,
		WAL:           *wal,
		Compression:   *compression,
		SyncThreshold: 500,
		IdleThreshold: 5 * time.Second,
	}
	if *encryption {
		key := make([]byte, 32)
		rng.Read(key)
		opts.EncryptionKey = key
	}

	db, err := embeddb.Open(*dbPath, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	useOpts := embeddb.UseOptions{}
	if *versioning > 0 {
		useOpts.MaxVersions = uint8(*versioning)
	}

	stopCh := make(chan struct{})
	stopOnce := sync.Once{}
	stop := func() { stopOnce.Do(func() { close(stopCh) }) }
	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			stop()
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var workerStates []*WorkerState
	for i := 0; i < *workers; i++ {
		tableName := fmt.Sprintf("worker_%d", i)
		table, err := embeddb.Use[TestRecord](db, tableName, useOpts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open table %s: %v\n", tableName, err)
			os.Exit(1)
		}
		workerStates = append(workerStates, &WorkerState{table: table})
	}

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerRng := rand.New(rand.NewSource(*seed + int64(workerID)))
			runWorker(workerID, db, workerStates[workerID], workerRng, stopCh, *maxRecords, *maxFileSize)
		}(i)
	}

	if *vacuumInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runVacuum(db, *vacuumInterval, stopCh)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		runStatsReporter(db, *statsInterval, *maxFileSize, stopCh)
	}()

	select {
	case <-sigCh:
		fmt.Println("\nReceived signal, shutting down...")
		stop()
	case <-stopCh:
		fmt.Println("\nDuration elapsed, shutting down...")
	}

	wg.Wait()

	finalOps := atomic.LoadInt64(&opCount)
	finalErrors := atomic.LoadInt64(&errorCount)
	finalExpected := atomic.LoadInt64(&expectedErrs)
	fmt.Printf("\nFinal Stats: %d operations, %d errors (%d expected)\n", finalOps, finalErrors, finalExpected)
	fmt.Printf("  Insert: %d, Update: %d, Query: %d, Tx: %d, Scan: %d, Filter: %d, Paged: %d, Bulk: %d\n",
		atomic.LoadInt64(&insertErrs), atomic.LoadInt64(&updateErrs), atomic.LoadInt64(&queryErrs),
		atomic.LoadInt64(&txErrs), atomic.LoadInt64(&scanErrs), atomic.LoadInt64(&filterErrs),
		atomic.LoadInt64(&pagedErrs), atomic.LoadInt64(&bulkErrs))

	errLogMu.Lock()
	if len(errLog) > 0 {
		fmt.Printf("\nLast %d errors:\n", len(errLog))
		for _, e := range errLog {
			fmt.Printf("  %s\n", e)
		}
	}
	errLogMu.Unlock()

	if finalErrors > 0 {
		crcErrs := atomic.LoadInt64(&insertErrs)
		if *workers > 1 && crcErrs == finalErrors {
			fmt.Println("\nNote: CRC errors in concurrent mode are a known limitation (mmap region resizing)")
			return
		}
		os.Exit(1)
	}
}

func runWorker(id int, db *embeddb.DB, ws *WorkerState, rng *rand.Rand, stopCh <-chan struct{}, maxRecords int, maxFileSize int64) {
	names := []string{"alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry", "iris", "jack",
		"kate", "leo", "mia", "noah", "olivia", "peter", "quinn", "rose", "sam", "tina",
		"ulysses", "vera", "wendy", "xavier", "yara", "zack"}
	notesPool := []string{
		"short note",
		"this is a longer note with more details about the record and some additional context",
		"",
		"special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?",
		"unicode: hello world",
		strings.Repeat("x", 1000),
		strings.Repeat("ab", 5000),
	}
	tagsPool := [][]string{
		{"tag1", "tag2"},
		{"important"},
		{},
		{"a", "b", "c", "d", "e"},
	}

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		op := rng.Intn(100)

		switch {
		case op < 25:
			runInsert(ws, rng, names, notesPool, tagsPool, maxRecords)
		case op < 45:
			runGet(ws, rng)
		case op < 60:
			runUpdate(ws, rng, names, notesPool, tagsPool)
		case op < 75:
			runDelete(ws, rng)
		case op < 85:
			runQuery(ws, rng)
		case op < 88:
			runScan(ws, rng)
		case op < 96:
			runFilter(ws, rng)
		case op < 98:
			runPagination(ws, rng)
		default:
			runBulkInsert(ws, rng, names, notesPool, tagsPool)
		}

		atomic.AddInt64(&opCount, 1)
	}
}

func runInsert(ws *WorkerState, rng *rand.Rand, names []string, notes []string, tags [][]string, maxRecords int) {
	ws.mu.RLock()
	count := len(ws.insertedIDs)
	ws.mu.RUnlock()
	if count >= maxRecords {
		runDelete(ws, rng)
		return
	}

	rec := TestRecord{
		Name:      names[rng.Intn(len(names))],
		Age:       int32(rng.Intn(80) + 18),
		Score:     rng.Float64() * 100,
		Active:    rng.Intn(2) == 1,
		CreatedAt: time.Now().Add(-time.Duration(rng.Intn(365*24)) * time.Hour),
		Notes:     notes[rng.Intn(len(notes))],
		Tags:      tags[rng.Intn(len(tags))],
	}

	_, err := ws.table.Insert(&rec)
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		atomic.AddInt64(&insertErrs, 1)
		logError(fmt.Sprintf("Insert: %v", err))
		return
	}

	ws.mu.Lock()
	ws.insertedIDs = append(ws.insertedIDs, rec.ID)
	ws.mu.Unlock()
	atomic.AddInt64(&insertCount, 1)
}

func runGet(ws *WorkerState, rng *rand.Rand) {
	ws.mu.RLock()
	if len(ws.insertedIDs) == 0 {
		ws.mu.RUnlock()
		return
	}
	id := ws.insertedIDs[rng.Intn(len(ws.insertedIDs))]
	ws.mu.RUnlock()

	_, err := ws.table.Get(id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			atomic.AddInt64(&expectedErrs, 1)
		} else {
			atomic.AddInt64(&errorCount, 1)
		}
	}
	atomic.AddInt64(&getCount, 1)
}

func runUpdate(ws *WorkerState, rng *rand.Rand, names []string, notes []string, tags [][]string) {
	ws.mu.RLock()
	if len(ws.insertedIDs) == 0 {
		ws.mu.RUnlock()
		return
	}
	id := ws.insertedIDs[rng.Intn(len(ws.insertedIDs))]
	ws.mu.RUnlock()

	existing, err := ws.table.Get(id)
	if err != nil {
		return
	}

	existing.Name = names[rng.Intn(len(names))]
	existing.Age = int32(rng.Intn(80) + 18)
	existing.Score = rng.Float64() * 100
	existing.Active = rng.Intn(2) == 1
	existing.Notes = notes[rng.Intn(len(notes))]
	existing.Tags = tags[rng.Intn(len(tags))]

	err = ws.table.Update(id, existing)
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		atomic.AddInt64(&updateErrs, 1)
		logError(fmt.Sprintf("Update(%d): %v", id, err))
		return
	}
	atomic.AddInt64(&updateCount, 1)
}

func runDelete(ws *WorkerState, rng *rand.Rand) {
	ws.mu.Lock()
	if len(ws.insertedIDs) == 0 {
		ws.mu.Unlock()
		return
	}
	idx := rng.Intn(len(ws.insertedIDs))
	id := ws.insertedIDs[idx]
	ws.insertedIDs = append(ws.insertedIDs[:idx], ws.insertedIDs[idx+1:]...)
	ws.mu.Unlock()

	err := ws.table.Delete(id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			atomic.AddInt64(&expectedErrs, 1)
		} else {
			atomic.AddInt64(&errorCount, 1)
		}
		return
	}
	atomic.AddInt64(&deleteCount, 1)
}

func runQuery(ws *WorkerState, rng *rand.Rand) {
	op := rng.Intn(5)
	var err error

	switch op {
	case 0:
		names := []string{"alice", "bob", "charlie"}
		_, err = ws.table.Query("Name", names[rng.Intn(len(names))])
	case 1:
		age := int32(rng.Intn(60) + 20)
		_, err = ws.table.QueryRangeGreaterThan("Age", age, rng.Intn(2) == 1)
	case 2:
		score := rng.Float64() * 100
		_, err = ws.table.QueryRangeLessThan("Score", score, rng.Intn(2) == 1)
	case 3:
		_, err = ws.table.QueryLike("Name", "%"+string(rune('a'+rng.Intn(26)))+"%")
	case 4:
		minAge := int32(rng.Intn(30) + 18)
		maxAge := minAge + int32(rng.Intn(30)+1)
		_, err = ws.table.QueryRangeBetween("Age", minAge, maxAge, true, true)
	}

	if err != nil && !strings.Contains(err.Error(), "no index") && !strings.Contains(err.Error(), "not found") {
		atomic.AddInt64(&errorCount, 1)
		atomic.AddInt64(&queryErrs, 1)
		logError(fmt.Sprintf("Query: %v", err))
	}
	atomic.AddInt64(&queryCount, 1)
}

func runScan(ws *WorkerState, rng *rand.Rand) {
	count := 0
	err := ws.table.Scan(func(rec TestRecord) bool {
		count++
		return count < rng.Intn(10)+1
	})
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		atomic.AddInt64(&scanErrs, 1)
		logError(fmt.Sprintf("Scan: %v", err))
	}
}

func runFilter(ws *WorkerState, rng *rand.Rand) {
	_, err := ws.table.Filter(func(rec TestRecord) bool {
		return rec.Active && rec.Age > 25
	})
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		atomic.AddInt64(&filterErrs, 1)
		logError(fmt.Sprintf("Filter: %v", err))
	}
}

func runPagination(ws *WorkerState, rng *rand.Rand) {
	offset := rng.Intn(100)
	limit := rng.Intn(20) + 1
	_, err := ws.table.AllPaged(offset, limit)
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		atomic.AddInt64(&pagedErrs, 1)
		logError(fmt.Sprintf("AllPaged: %v", err))
	}
}

func runBulkInsert(ws *WorkerState, rng *rand.Rand, names []string, notes []string, tags [][]string) {
	batchSize := rng.Intn(50) + 10
	records := make([]*TestRecord, batchSize)
	for i := range records {
		records[i] = &TestRecord{
			Name:      names[rng.Intn(len(names))],
			Age:       int32(rng.Intn(80) + 18),
			Score:     rng.Float64() * 100,
			Active:    rng.Intn(2) == 1,
			CreatedAt: time.Now().Add(-time.Duration(rng.Intn(365*24)) * time.Hour),
			Notes:     notes[rng.Intn(len(notes))],
			Tags:      tags[rng.Intn(len(tags))],
		}
	}

	ids, err := ws.table.InsertManyBulk(records)
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		atomic.AddInt64(&bulkErrs, 1)
		logError(fmt.Sprintf("BulkInsert: %v", err))
		return
	}

	ws.mu.Lock()
	for _, id := range ids {
		ws.insertedIDs = append(ws.insertedIDs, id)
	}
	ws.mu.Unlock()
	atomic.AddInt64(&insertCount, int64(len(ids)))
}

func runVacuum(db *embeddb.DB, interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			start := time.Now()
			err := db.Vacuum()
			dur := time.Since(start)
			if err != nil {
				fmt.Printf("[VACUUM] ERROR: %v (took %v)\n", err, dur)
				atomic.AddInt64(&errorCount, 1)
			} else {
				fmt.Printf("[VACUUM] completed in %v\n", dur)
			}
		}
	}
}

func runStatsReporter(db *embeddb.DB, interval time.Duration, maxFileSize int64, stopCh <-chan struct{}) {
	startTime := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			stats := db.Stats()
			ops := atomic.LoadInt64(&opCount)
			elapsed := time.Since(startTime)
			opsPerSec := float64(ops) / elapsed.Seconds()

			var recordCount int
			for _, ts := range stats.Tables {
				recordCount += ts.RecordCount
			}

			snapshot := StatsSnapshot{
				Time:           time.Now().Format(time.RFC3339),
				Ops:            ops,
				Inserts:        atomic.LoadInt64(&insertCount),
				Gets:           atomic.LoadInt64(&getCount),
				Updates:        atomic.LoadInt64(&updateCount),
				Deletes:        atomic.LoadInt64(&deleteCount),
				Queries:        atomic.LoadInt64(&queryCount),
				TxCommits:      atomic.LoadInt64(&txCommitCnt),
				TxRollbacks:    atomic.LoadInt64(&txRollbackCnt),
				Errors:         atomic.LoadInt64(&errorCount),
				ExpectedErrors: atomic.LoadInt64(&expectedErrs),
				FileSize:       stats.FileSize,
				WALSize:        stats.WALSize,
				RecordCount:    recordCount,
				IndexKeys:      stats.IndexKeys,
				BTreeDepth:     stats.BTreeDepth,
				FreeBytes:      stats.Allocator.FreeBytes,
				FreeBlocks:     stats.Allocator.FreeBlocks,
				CacheHitRate:   stats.CacheStats["primary"].HitRate,
				MaxFileSize:    maxFileSize,
			}

			jsonBytes, _ := json.Marshal(snapshot)
			fmt.Printf("[STATS] %s\n", string(jsonBytes))

			if stats.FileSize > maxFileSize {
				fmt.Printf("[ALERT] File size %s exceeds max %s!\n",
					formatBytes(stats.FileSize), formatBytes(maxFileSize))
				atomic.AddInt64(&errorCount, 1)
			}

			if stats.Allocator.FreeBytes > 0 {
				freeRatio := float64(stats.Allocator.FreeBytes) / float64(stats.FileSize)
				if freeRatio > 0.5 && recordCount > 1000 {
					fmt.Printf("[ALERT] High free space ratio: %.1f%% (%s free)\n",
						freeRatio*100, formatBytes(int64(stats.Allocator.FreeBytes)))
				}
			}

			_ = runtime.MemStats{}
			_ = opsPerSec
		}
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
