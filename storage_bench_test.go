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

func benchInsert(mode StorageMode, label string, count int, useWAL bool) (time.Duration, uint64, int64) {
	dbPath := fmt.Sprintf("/tmp/bench_%s_%d.db", label, count)
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	memBefore := measureMem()

	db, err := Open(dbPath, OpenOptions{StorageMode: mode, WAL: useWAL})
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
	os.Remove(dbPath + ".wal")

	return elapsed, heapAlloc, fileSize
}

func benchRead(mode StorageMode, label string, count int, useWAL bool) time.Duration {
	dbPath := fmt.Sprintf("/tmp/bench_%s_%d.db", label, count)
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, _ := Open(dbPath, OpenOptions{StorageMode: mode, WAL: useWAL})
	tbl, _ := Use[BenchRecord](db, "bench")
	for i := 0; i < count; i++ {
		tbl.Insert(&BenchRecord{
			Name:  fmt.Sprintf("item_%d", i),
			Value: int64(i),
			Data:  fmt.Sprintf("data_payload_%d", i),
		})
	}
	db.Close()

	db, _ = Open(dbPath, OpenOptions{StorageMode: mode, WAL: useWAL})
	tbl, _ = Use[BenchRecord](db, "bench")

	start := time.Now()
	for i := 0; i < count; i++ {
		tbl.Get(uint32(i + 1))
	}
	elapsed := time.Since(start)

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	return elapsed
}

func benchQuery(mode StorageMode, label string, count int, useWAL bool) time.Duration {
	dbPath := fmt.Sprintf("/tmp/bench_%s_%d.db", label, count)
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	db, _ := Open(dbPath, OpenOptions{StorageMode: mode, WAL: useWAL})
	tbl, _ := Use[BenchRecord](db, "bench")
	for i := 0; i < count; i++ {
		tbl.Insert(&BenchRecord{
			Name:  fmt.Sprintf("item_%d", i),
			Value: int64(i),
			Data:  fmt.Sprintf("data_payload_%d", i),
		})
	}
	db.Close()

	db, _ = Open(dbPath, OpenOptions{StorageMode: mode, WAL: useWAL})
	tbl, _ = Use[BenchRecord](db, "bench")

	start := time.Now()
	for i := 0; i < 100; i++ {
		tbl.Query("Name", fmt.Sprintf("item_%d", i*count/100))
	}
	elapsed := time.Since(start)

	db.Close()
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")

	return elapsed
}

func TestStorageBenchmark(t *testing.T) {
	counts := []int{10_000, 100_000}

	for _, count := range counts {
		label := fmt.Sprintf("%dk", count/1000)

		t.Logf("\n=== [%s entries] ===\n", label)

		mmapInsertTime, mmapInsertMem, mmapFileSize := benchInsert(StorageMmap, "mmap", count, false)
		mmapWalInsertTime, mmapWalInsertMem, mmapWalFileSize := benchInsert(StorageMmap, "mmap_wal", count, true)
		fileInsertTime, fileInsertMem, fileFileSize := benchInsert(StorageFile, "file", count, false)
		fileWalInsertTime, fileWalInsertMem, fileWalFileSize := benchInsert(StorageFile, "file_wal", count, true)

		mmapReadTime := benchRead(StorageMmap, "mmap", count, false)
		mmapWalReadTime := benchRead(StorageMmap, "mmap_wal", count, true)
		fileReadTime := benchRead(StorageFile, "file", count, false)
		fileWalReadTime := benchRead(StorageFile, "file_wal", count, true)

		mmapQueryTime := benchQuery(StorageMmap, "mmap", count, false)
		mmapWalQueryTime := benchQuery(StorageMmap, "mmap_wal", count, true)
		fileQueryTime := benchQuery(StorageFile, "file", count, false)
		fileWalQueryTime := benchQuery(StorageFile, "file_wal", count, true)

		t.Logf("Insert time:    mmap=%v  mmap+wal=%v(%.1fx)  file=%v  file+wal=%v(%.1fx)",
			mmapInsertTime, mmapWalInsertTime, float64(mmapWalInsertTime)/float64(mmapInsertTime),
			fileInsertTime, fileWalInsertTime, float64(fileWalInsertTime)/float64(fileInsertTime))
		t.Logf("Insert memory:  mmap=%dMB  mmap+wal=%dMB  file=%dMB  file+wal=%dMB",
			mmapInsertMem/1024/1024, mmapWalInsertMem/1024/1024,
			fileInsertMem/1024/1024, fileWalInsertMem/1024/1024)
		t.Logf("File size:      mmap=%dMB(+%dMB wal)  file=%dMB(+%dMB wal)",
			mmapFileSize/1024/1024, (mmapWalFileSize-mmapFileSize)/1024/1024,
			fileFileSize/1024/1024, (fileWalFileSize-fileFileSize)/1024/1024)
		t.Logf("Read all:       mmap=%v  mmap+wal=%v  file=%v  file+wal=%v",
			mmapReadTime, mmapWalReadTime, fileReadTime, fileWalReadTime)
		t.Logf("Query 100:      mmap=%v  mmap+wal=%v  file=%v  file+wal=%v",
			mmapQueryTime, mmapWalQueryTime, fileQueryTime, fileWalQueryTime)
	}
}
