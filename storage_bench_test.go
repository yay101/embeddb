package embeddb

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

type BenchRecord struct {
	ID    uint32 `db:"id,primary"`
	Name  string `db:"index"`
	Value int64  `db:"index"`
	Data  string
}

func measureMem() uint64 {
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	return ms.TotalAlloc
}

func benchInsert(mode StorageMode, label string, count int) (time.Duration, uint64, int64) {
	dbPath := fmt.Sprintf("/tmp/bench_%s_%d.db", label, count)
	os.Remove(dbPath)

	memBefore := measureMem()

	db, err := Open(dbPath, OpenOptions{StorageMode: mode})
	if err != nil {
		panic(err)
	}

	tbl, err := Use[BenchRecord](db, "bench")
	if err != nil {
		panic(err)
	}

	start := time.Now()
	for i := 0; i < count; i++ {
		_, err := tbl.Insert(&BenchRecord{
			Name:  fmt.Sprintf("item_%d", i),
			Value: int64(i),
			Data:  fmt.Sprintf("data_payload_for_item_%d_with_some_extra_length_to_simulate_real_data", i),
		})
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)

	memAfter := measureMem()
	heapAlloc := memAfter - memBefore

	db.Close()

	fileInfo, _ := os.Stat(dbPath)
	fileSize := fileInfo.Size()

	os.Remove(dbPath)

	return elapsed, heapAlloc, fileSize
}

func benchRead(mode StorageMode, label string, count int) time.Duration {
	dbPath := fmt.Sprintf("/tmp/bench_%s_%d.db", label, count)
	os.Remove(dbPath)

	db, _ := Open(dbPath, OpenOptions{StorageMode: mode})
	tbl, _ := Use[BenchRecord](db, "bench")
	for i := 0; i < count; i++ {
		tbl.Insert(&BenchRecord{
			Name:  fmt.Sprintf("item_%d", i),
			Value: int64(i),
			Data:  fmt.Sprintf("data_payload_%d", i),
		})
	}
	db.Close()

	db, _ = Open(dbPath, OpenOptions{StorageMode: mode})
	tbl, _ = Use[BenchRecord](db, "bench")

	start := time.Now()
	for i := 0; i < count; i++ {
		tbl.Get(uint32(i + 1))
	}
	elapsed := time.Since(start)

	db.Close()
	os.Remove(dbPath)

	return elapsed
}

func benchQuery(mode StorageMode, label string, count int) time.Duration {
	dbPath := fmt.Sprintf("/tmp/bench_%s_%d.db", label, count)
	os.Remove(dbPath)

	db, _ := Open(dbPath, OpenOptions{StorageMode: mode})
	tbl, _ := Use[BenchRecord](db, "bench")
	for i := 0; i < count; i++ {
		tbl.Insert(&BenchRecord{
			Name:  fmt.Sprintf("item_%d", i),
			Value: int64(i),
			Data:  fmt.Sprintf("data_payload_%d", i),
		})
	}
	db.Close()

	db, _ = Open(dbPath, OpenOptions{StorageMode: mode})
	tbl, _ = Use[BenchRecord](db, "bench")

	start := time.Now()
	for i := 0; i < 100; i++ {
		tbl.Query("Name", fmt.Sprintf("item_%d", i*count/100))
	}
	elapsed := time.Since(start)

	db.Close()
	os.Remove(dbPath)

	return elapsed
}

func TestStorageBenchmark(t *testing.T) {
	counts := []int{10_000, 100_000, 300_000}

	t.Logf("\n%-20s | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s",
		"Operation", "10k Mmap", "10k File", "100k Mmap", "100k File", "300k Mmap", "300k File", "Delta")
	t.Logf("%s", "-------------------|--------------|--------------|--------------|--------------|--------------|--------------|--------------")

	for _, count := range counts {
		label := fmt.Sprintf("%dk", count/1000)

		mmapInsertTime, mmapInsertMem, mmapFileSize := benchInsert(StorageMmap, "mmap", count)
		fileInsertTime, fileInsertMem, fileFileSize := benchInsert(StorageFile, "file", count)

		mmapReadTime := benchRead(StorageMmap, "mmap", count)
		fileReadTime := benchRead(StorageFile, "file", count)

		mmapQueryTime := benchQuery(StorageMmap, "mmap", count)
		fileQueryTime := benchQuery(StorageFile, "file", count)

		t.Logf("\n[%s entries]\n", label)
		t.Logf("Insert time:    mmap=%v  file=%v  (file %.1fx)", mmapInsertTime, fileInsertTime, float64(fileInsertTime)/float64(mmapInsertTime))
		t.Logf("Insert memory:  mmap=%dMB  file=%dMB", mmapInsertMem/1024/1024, fileInsertMem/1024/1024)
		t.Logf("File size:      mmap=%dMB  file=%dMB", mmapFileSize/1024/1024, fileFileSize/1024/1024)
		t.Logf("Read all:       mmap=%v  file=%v  (file %.1fx)", mmapReadTime, fileReadTime, float64(fileReadTime)/float64(mmapReadTime))
		t.Logf("Query 100:      mmap=%v  file=%v  (file %.1fx)", mmapQueryTime, fileQueryTime, float64(fileQueryTime)/float64(mmapQueryTime))
	}
}
