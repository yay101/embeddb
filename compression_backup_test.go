package embeddb

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

type CompressRecord struct {
	ID    uint32 `db:"id,primary"`
	Name  string
	Data  string
	Notes string
}

func TestCompressionBasic(t *testing.T) {
	os.Remove("/tmp/test_compress_basic.db")
	defer os.Remove("/tmp/test_compress_basic.db")

	db, err := Open("/tmp/test_compress_basic.db", OpenOptions{
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err := Use[CompressRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	largeData := strings.Repeat("abcdefghij", 100)
	_, err = tbl.Insert(&CompressRecord{
		Name:  "test",
		Data:  largeData,
		Notes: "some notes",
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tbl.Get(uint32(1))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Data != largeData {
		t.Errorf("data mismatch: got %d bytes, expected %d bytes", len(rec.Data), len(largeData))
	}
	if rec.Name != "test" {
		t.Errorf("name mismatch: got %s, expected test", rec.Name)
	}
}

func TestCompressionRestart(t *testing.T) {
	os.Remove("/tmp/test_compress_restart.db")
	defer os.Remove("/tmp/test_compress_restart.db")

	db, err := Open("/tmp/test_compress_restart.db", OpenOptions{
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := Use[CompressRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	largeData := strings.Repeat("xyz123", 200)
	for i := 0; i < 50; i++ {
		_, err := tbl.Insert(&CompressRecord{
			Name:  fmt.Sprintf("item%d", i),
			Data:  largeData,
			Notes: fmt.Sprintf("note%d", i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Close()

	db, err = Open("/tmp/test_compress_restart.db", OpenOptions{
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tbl, err = Use[CompressRecord](db, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(1); i <= 50; i++ {
		rec, err := tbl.Get(i)
		if err != nil {
			t.Fatalf("failed to get record %d: %v", i, err)
		}
		if rec.Data != largeData {
			t.Errorf("record %d: data mismatch", i)
		}
	}
}

func TestCompressionSizeReduction(t *testing.T) {
	os.Remove("/tmp/test_compress_size.db")
	defer os.Remove("/tmp/test_compress_size.db")

	os.Remove("/tmp/test_nocompress_size.db")
	defer os.Remove("/tmp/test_nocompress_size.db")

	type SizeRecord struct {
		ID    uint32 `db:"id,primary"`
		Name  string
		Data  string
		Notes string
	}

	largeData := strings.Repeat("the quick brown fox jumps over the lazy dog. ", 100)

	dbNo, _ := Open("/tmp/test_nocompress_size.db", OpenOptions{
		AutoIndex: false,
	})
	tblNo, _ := Use[SizeRecord](dbNo, "test")
	for i := 0; i < 100; i++ {
		tblNo.Insert(&SizeRecord{
			Name:  fmt.Sprintf("item%d", i),
			Data:  largeData,
			Notes: "notes",
		})
	}
	dbNo.database.flush()
	dbNo.Close()

	dbYes, _ := Open("/tmp/test_compress_size.db", OpenOptions{
		Compression: true,
		AutoIndex:   false,
	})
	tblYes, _ := Use[SizeRecord](dbYes, "test")
	for i := 0; i < 100; i++ {
		tblYes.Insert(&SizeRecord{
			Name:  fmt.Sprintf("item%d", i),
			Data:  largeData,
			Notes: "notes",
		})
	}
	dbYes.database.flush()
	dbYes.Close()

	statNo, _ := os.Stat("/tmp/test_nocompress_size.db")
	statYes, _ := os.Stat("/tmp/test_compress_size.db")

	reduction := float64(statNo.Size()-statYes.Size()) / float64(statNo.Size()) * 100
	t.Logf("No compression: %d bytes, With compression: %d bytes (%.1f%% reduction)",
		statNo.Size(), statYes.Size(), reduction)

	if statYes.Size() >= statNo.Size() {
		t.Errorf("compressed file should be smaller")
	}
}

func TestBackupBasic(t *testing.T) {
	os.Remove("/tmp/test_backup_src.db")
	os.Remove("/tmp/test_backup_src.db.wal")
	os.Remove("/tmp/test_backup_dst.db")
	os.Remove("/tmp/test_backup_dst.db.wal")
	defer func() {
		os.Remove("/tmp/test_backup_src.db")
		os.Remove("/tmp/test_backup_src.db.wal")
		os.Remove("/tmp/test_backup_dst.db")
		os.Remove("/tmp/test_backup_dst.db.wal")
	}()

	db, _ := Open("/tmp/test_backup_src.db", OpenOptions{
		WAL: true,
	})
	tbl, _ := Use[CompressRecord](db, "test")

	for i := 0; i < 50; i++ {
		tbl.Insert(&CompressRecord{
			Name:  fmt.Sprintf("item%d", i),
			Data:  strings.Repeat("data", 50),
			Notes: "notes",
		})
	}

	err := db.Backup("/tmp/test_backup_dst.db")
	if err != nil {
		t.Fatal(err)
	}

	dstDB, err := Open("/tmp/test_backup_dst.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dstDB.Close()

	dstTbl, err := Use[CompressRecord](dstDB, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := uint32(1); i <= 50; i++ {
		rec, err := dstTbl.Get(i)
		if err != nil {
			t.Fatalf("backup: failed to get record %d: %v", i, err)
		}
		if rec.Name != fmt.Sprintf("item%d", i-1) {
			t.Errorf("backup: record %d name mismatch", i)
		}
	}
}

func TestBackupClosedDB(t *testing.T) {
	os.Remove("/tmp/test_backup_closed.db")
	defer os.Remove("/tmp/test_backup_closed.db")

	db, _ := Open("/tmp/test_backup_closed.db")
	db.Close()

	err := db.Backup("/tmp/test_backup_closed_dst.db")
	if err == nil {
		t.Error("expected error for closed database")
	}
}
