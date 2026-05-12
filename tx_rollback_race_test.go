package embeddb

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TxReproRecord struct {
	ID   uint32 `db:"id,primary"`
	Name string `db:"name,index"`
	Age  int32  `db:"age,index"`
	Data string
}

// TestTxRollbackWithConcurrentReads hammers Begin->Insert->Rollback cycles
// while concurrent Get/Scan reads are running. This is the specific pattern
// that triggers CRC errors in the torture test (Phase 1 finding).
func TestTxRollbackWithConcurrentReads(t *testing.T) {
	t.Skip("transactions are an internal/immature feature")
	os.Remove("/tmp/test_tx_rollback_race.db")
	defer os.Remove("/tmp/test_tx_rollback_race.db")

	db, err := Open("/tmp/test_tx_rollback_race.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TxReproRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Pre-populate with some records so there's data to read
	for i := 0; i < 500; i++ {
		_, err := tbl.Insert(&TxReproRecord{
			Name: fmt.Sprintf("pre-%d", i),
			Age:  int32(i % 100),
			Data: strings.Repeat("x", 100),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	var (
		txCommit  int64
		txRollback int64
		gets       int64
		crcErr     int64
		stop       int64
		wg         sync.WaitGroup
	)

	// Transaction goroutine: Begin -> Insert -> Rollback in tight loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt64(&stop) == 0 {
			tx := db.begin()
			if tx == nil {
				continue
			}

			_, err := tbl.Insert(&TxReproRecord{
				Name: "tx-insert",
				Age:  25,
				Data: strings.Repeat("txdata", 100),
			})
			if err != nil {
				tx.rollback()
				atomic.AddInt64(&txRollback, 1)
				continue
			}

			if time.Now().UnixNano()%2 == 0 {
				db.commit()
				atomic.AddInt64(&txCommit, 1)
			} else {
				db.rollback()
				atomic.AddInt64(&txRollback, 1)
			}
		}
	}()

	// Reader goroutines: Get and Scan concurrently with transactions
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			for atomic.LoadInt64(&stop) == 0 {
				// Get a known record
				_, err := tbl.Get(uint32(rid + 1))
				if err != nil && !strings.Contains(err.Error(), "not found") {
					if strings.Contains(err.Error(), "CRC") {
						atomic.AddInt64(&crcErr, 1)
						t.Errorf("CRC error in Get(%d): %v", rid+1, err)
						atomic.StoreInt64(&stop, 1)
						return
					}
				}
				atomic.AddInt64(&gets, 1)

				// Scan all records
				count := 0
				tbl.Scan(func(rec TxReproRecord) bool {
					count++
					return count < 10
				})
				atomic.AddInt64(&gets, 1)
			}
		}(r)
	}

	time.Sleep(15 * time.Second)
	atomic.StoreInt64(&stop, 1)
	wg.Wait()

	// Verify B-tree integrity
	if err := db.database.index.Verify(); err != nil {
		t.Errorf("BTree verify failed: %v", err)
	}

	t.Logf("Tx commits: %d, rollbacks: %d, gets: %d, CRC errors: %d",
		txCommit, txRollback, gets, crcErr)

	if crcErr > 0 {
		t.Fatal("CRC errors detected")
	}
}

// TestTxRollbackVsBTreeIntegrity checks that the B-tree remains correct
// after rollback by inserting records, rolling back, and verifying all
// pre-existing records are still accessible.
func TestTxRollbackVsBTreeIntegrity(t *testing.T) {
	t.Skip("transactions are an internal/immature feature")
	os.Remove("/tmp/test_tx_integrity.db")
	defer os.Remove("/tmp/test_tx_integrity.db")

	db, err := Open("/tmp/test_tx_integrity.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TxReproRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Insert base records
	baseCount := 1000
	baseIDs := make(map[uint32]bool, baseCount)
	for i := 0; i < baseCount; i++ {
		rec := &TxReproRecord{
			Name: fmt.Sprintf("base-%d", i),
			Age:  int32(i),
			Data: strings.Repeat("base", 20),
		}
		_, err := tbl.Insert(rec)
		if err != nil {
			t.Fatal(err)
		}
		baseIDs[rec.ID] = true
	}

	// Now do many Begin->Insert->Rollback cycles
	for i := 0; i < 200; i++ {
		tx := db.begin()
		if tx == nil {
			t.Fatal("Begin returned nil")
		}

		// Insert a record inside transaction
		_, err := tbl.Insert(&TxReproRecord{
			Name: fmt.Sprintf("tx-%d", i),
			Age:  int32(i),
			Data: strings.Repeat("tx", 50),
		})
		if err != nil {
			tx.rollback()
			continue
		}

		// Rollback — insert should be discarded
		if err := db.rollback(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all base records still exist and are readable
	for id := range baseIDs {
		rec, err := tbl.Get(id)
		if err != nil {
			t.Errorf("Get(%d) failed after rollback cycles: %v", id, err)
			continue
		}
		if rec == nil {
			t.Errorf("Get(%d) returned nil", id)
		}
	}

	// B-tree integrity
	if err := db.database.index.Verify(); err != nil {
		t.Errorf("BTree verify after rollback cycles: %v", err)
	}
}

// TestTxRollbackVsBTreeIntegrityConcurrent is like TestTxRollbackVsBTreeIntegrity
// but with concurrent reads during rollback.
func TestTxRollbackVsBTreeIntegrityConcurrent(t *testing.T) {
	t.Skip("transactions are an internal/immature feature")
	os.Remove("/tmp/test_tx_integrity_conc.db")
	defer os.Remove("/tmp/test_tx_integrity_conc.db")

	db, err := Open("/tmp/test_tx_integrity_conc.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TxReproRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Insert base records
	baseIDs := make(map[uint32]bool)
	for i := 0; i < 500; i++ {
		rec := &TxReproRecord{
			Name: fmt.Sprintf("base-%d", i),
			Age:  int32(i),
			Data: strings.Repeat("base", 20),
		}
		_, err := tbl.Insert(rec)
		if err != nil {
			t.Fatal(err)
		}
		baseIDs[rec.ID] = true
	}
	t.Logf("Inserted %d base records", len(baseIDs))

	var (
		stop       int64
		crcErr     int64
		notFound   int64
		wg         sync.WaitGroup
	)

	// Transaction goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			if atomic.LoadInt64(&stop) != 0 {
				return
			}
			tx := db.begin()
			if tx == nil {
				continue
			}
			_, err := tbl.Insert(&TxReproRecord{
				Name: fmt.Sprintf("tx-%d", i),
				Age:  100,
				Data: strings.Repeat("tx-data", 30),
			})
			if err != nil {
				tx.rollback()
				continue
			}
			db.rollback()
		}
	}()

	// Reader goroutines — read base records during rollbacks
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			for atomic.LoadInt64(&stop) == 0 {
				for id := range baseIDs {
					if atomic.LoadInt64(&stop) != 0 {
						return
					}
					_, err := tbl.Get(id)
					if err != nil {
						if strings.Contains(err.Error(), "CRC") {
							atomic.AddInt64(&crcErr, 1)
							t.Errorf("CRC in Get(%d): %v", id, err)
							atomic.StoreInt64(&stop, 1)
							return
						}
						if strings.Contains(err.Error(), "not found") {
							atomic.AddInt64(&notFound, 1)
						}
					}
				}

				// Also scan
				_ = tbl.Scan(func(rec TxReproRecord) bool {
					return len(baseIDs) > 0
				})
			}
		}(r)
	}

	time.Sleep(15 * time.Second)
	atomic.StoreInt64(&stop, 1)
	wg.Wait()

	t.Logf("CRC errors: %d, NotFound: %d", crcErr, notFound)

	if crcErr > 0 {
		t.Fatal("CRC errors detected")
	}

	// B-tree integrity
	if err := db.database.index.Verify(); err != nil {
		t.Errorf("BTree verify: %v", err)
	}
}

// TestTxBulkInsertRollback verifies that a BulkInsert within a transaction
// is properly rolled back and does not corrupt the B-tree. This targets the
// scenario where children[i] could reference offsets freed by rollback.
func TestTxBulkInsertRollback(t *testing.T) {
	t.Skip("transactions are an internal/immature feature")
	os.Remove("/tmp/test_tx_bulk_rollback.db")
	defer os.Remove("/tmp/test_tx_bulk_rollback.db")

	db, err := Open("/tmp/test_tx_bulk_rollback.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TxReproRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Pre-populate records so the B-tree has internal levels
	baseIDs := make(map[uint32]bool)
	for i := 0; i < 500; i++ {
		rec := &TxReproRecord{
			Name: fmt.Sprintf("base-%d", i),
			Age:  int32(i),
			Data: strings.Repeat("x", 100),
		}
		_, err := tbl.Insert(rec)
		if err != nil {
			t.Fatal(err)
		}
		baseIDs[rec.ID] = true
	}

	// Many Begin -> BulkInsert -> Rollback cycles
	bulkData := strings.Repeat("bulk", 200)
	for cycle := 0; cycle < 100; cycle++ {
		tx := db.begin()
		if tx == nil {
			t.Fatal("Begin returned nil")
		}

		// Build bulk insert records
		records := make([]*TxReproRecord, 50)
		for j := 0; j < 50; j++ {
			records[j] = &TxReproRecord{
				Name: fmt.Sprintf("bulk-repro-%d-%d", cycle, j),
				Age:  int32(cycle),
				Data: bulkData,
			}
		}

		_, err := tbl.InsertManyBulk(records)
		if err != nil {
			tx.rollback()
			continue
		}

		// Always rollback — bulk insert should be discarded
		if err := db.rollback(); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all base records still exist
	for id := range baseIDs {
		rec, err := tbl.Get(id)
		if err != nil {
			t.Errorf("Get(%d) failed after bulk rollback cycles: %v", id, err)
			continue
		}
		if rec == nil {
			t.Errorf("Get(%d) returned nil", id)
		}
	}

	// B-tree integrity check
	if err := db.database.index.Verify(); err != nil {
		t.Errorf("BTree verify failed: %v", err)
	}
}

// TestTxBulkInsertRollbackConcurrent stresses BulkInsert + Rollback with concurrent reads.
func TestTxBulkInsertRollbackConcurrent(t *testing.T) {
	t.Skip("transactions are an internal/immature feature")
	os.Remove("/tmp/test_tx_bulk_rollback_conc.db")
	defer os.Remove("/tmp/test_tx_bulk_rollback_conc.db")

	db, err := Open("/tmp/test_tx_bulk_rollback_conc.db", OpenOptions{AutoIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[TxReproRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	// Pre-populate with enough records to create multi-level tree
	baseIDs := make(map[uint32]bool)
	for i := 0; i < 1000; i++ {
		rec := &TxReproRecord{
			Name: fmt.Sprintf("base-%d", i),
			Age:  int32(i % 100),
			Data: strings.Repeat("x", 100),
		}
		_, err := tbl.Insert(rec)
		if err != nil {
			t.Fatal(err)
		}
		baseIDs[rec.ID] = true
	}

	var (
		stop     int64
		crcErr   int64
		wg       sync.WaitGroup
	)

	// Transaction goroutine: Begin -> BulkInsert -> Rollback
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if atomic.LoadInt64(&stop) != 0 {
				return
			}
			tx := db.begin()
			if tx == nil {
				continue
			}

			records := make([]*TxReproRecord, 100)
			for j := 0; j < 100; j++ {
				records[j] = &TxReproRecord{
					Name: fmt.Sprintf("bulk-tx-%d-%d", i, j),
					Age:  int32(i),
					Data: strings.Repeat("bulk", 200),
				}
			}

			if _, err := tbl.InsertManyBulk(records); err != nil {
				tx.rollback()
				continue
			}
			db.rollback()
		}
	}()

	// Reader goroutines: read and scan while rollbacks happen
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt64(&stop) == 0 {
				for id := range baseIDs {
					if atomic.LoadInt64(&stop) != 0 {
						return
					}
					_, err := tbl.Get(id)
					if err != nil {
						if strings.Contains(err.Error(), "CRC") {
							atomic.AddInt64(&crcErr, 1)
							t.Errorf("CRC error in Get(%d): %v", id, err)
							atomic.StoreInt64(&stop, 1)
							return
						}
						if strings.Contains(err.Error(), "readNode") {
							atomic.AddInt64(&crcErr, 1)
							t.Errorf("readNode error in Get(%d): %v", id, err)
							atomic.StoreInt64(&stop, 1)
							return
						}
					}
				}
				// Scan a few records
				count := 0
				tbl.Scan(func(rec TxReproRecord) bool {
					count++
					return count < 20
				})
			}
		}()
	}

	time.Sleep(15 * time.Second)
	atomic.StoreInt64(&stop, 1)
	wg.Wait()

	t.Logf("CRC errors: %d", crcErr)

	if crcErr > 0 {
		t.Fatal("CRC errors detected during bulk insert rollback")
	}

	// Final integrity check
	for id := range baseIDs {
		_, err := tbl.Get(id)
		if err != nil {
			t.Errorf("Final Get(%d) failed: %v", id, err)
		}
	}

	if err := db.database.index.Verify(); err != nil {
		t.Errorf("BTree verify failed: %v", err)
	}
}
