package embeddb

import (
	"os"
	"testing"

	"github.com/yay101/embeddbmmap"
)

func TestAllocateBasic(t *testing.T) {
	f, err := os.CreateTemp("", "alloc_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	filename := f.Name()
	defer os.Remove(filename)

	f.Truncate(4096)
	a := newAllocator(f)

	off1, len1, err := a.Allocate(100)
	if err != nil {
		t.Fatalf("first allocate failed: %v", err)
	}
	if off1 != 4096 {
		t.Errorf("first offset: got %d, want 4096", off1)
	}
	if len1 != 100 {
		t.Errorf("first length: got %d, want 100", len1)
	}

	off2, len2, err := a.Allocate(200)
	if err != nil {
		t.Fatalf("second allocate failed: %v", err)
	}
	if off2 != 4096+100 {
		t.Errorf("second offset: got %d, want %d", off2, 4096+100)
	}
	if len2 != 200 {
		t.Errorf("second length: got %d, want 200", len2)
	}

	a.Free(off1, 100)

	off3, len3, err := a.Allocate(50)
	if err != nil {
		t.Fatalf("reuse allocate failed: %v", err)
	}
	if off3 != 4096 {
		t.Errorf("reuse offset: got %d, want 4096", off3)
	}
	if len3 != 50 {
		t.Errorf("reuse length: got %d, want 50", len3)
	}

	f.Close()
}

func TestAllocateFreesCoalesce(t *testing.T) {
	f, err := os.CreateTemp("", "alloc_coalesce_*.db")
	if err != nil {
		t.Fatal(err)
	}
	filename := f.Name()
	defer os.Remove(filename)

	f.Truncate(4096)
	a := newAllocator(f)

	off1, _, _ := a.Allocate(100)
	off2, _, _ := a.Allocate(100)
	_, _, _ = a.Allocate(100)

	a.Free(off1, 100)
	a.Free(off2, 100)

	freelist := a.CopyFreeList()
	if len(freelist) != 1 {
		t.Errorf("expected 1 coalesced block, got %d", len(freelist))
	} else if freelist[0].length != 200 {
		t.Errorf("expected coalesced length 200, got %d", freelist[0].length)
	}

	f.Close()
}

func TestAllocateTruncateError(t *testing.T) {
	f, err := os.CreateTemp("", "alloc_trunc_*.db")
	if err != nil {
		t.Fatal(err)
	}
	filename := f.Name()
	defer os.Remove(filename)

	f.Truncate(4096)

	a := newAllocator(f)

	region, err := embeddbmmap.Map(int(f.Fd()), 0, 4096, embeddbmmap.ProtRead|embeddbmmap.ProtWrite, embeddbmmap.MapShared)
	if err != nil {
		t.Skipf("mmap not available: %v", err)
	}
	a.region.Store(region)

	f.Close()
	os.Remove(f.Name())

	_, _, err = a.Allocate(1024 * 1024)
	if err == nil {
		t.Error("expected allocate to fail when file backing mmap is deleted")
	}
}

func TestAllocateFromFreeList(t *testing.T) {
	f, err := os.CreateTemp("", "alloc_freelist_*.db")
	if err != nil {
		t.Fatal(err)
	}
	filename := f.Name()
	defer os.Remove(filename)

	f.Truncate(4096)
	a := newAllocator(f)

	off, _, _ := a.Allocate(300)
	a.Allocate(100)
	a.Free(off, 300)

	off2, len2, err := a.Allocate(150)
	if err != nil {
		t.Fatalf("allocate from free list failed: %v", err)
	}
	if off2 != off {
		t.Errorf("expected reuse at offset %d, got %d", off, off2)
	}
	if len2 != 150 {
		t.Errorf("length from free list: got %d, want 150", len2)
	}

	freelist := a.CopyFreeList()
	if len(freelist) != 1 {
		t.Errorf("expected 1 remaining free block, got %d", len(freelist))
	} else if freelist[0].length != 150 {
		t.Errorf("remaining free block length: got %d, want 150", freelist[0].length)
	}

	f.Close()
}

func TestAllocateRollbackOnTruncateFail(t *testing.T) {
	f, err := os.CreateTemp("", "alloc_rollback_*.db")
	if err != nil {
		t.Fatal(err)
	}
	filename := f.Name()
	defer os.Remove(filename)

	f.Truncate(4096)
	a := newAllocator(f)

	region, err := embeddbmmap.Map(int(f.Fd()), 0, 4096, embeddbmmap.ProtRead|embeddbmmap.ProtWrite, embeddbmmap.MapShared)
	if err != nil {
		t.Skipf("mmap not available: %v", err)
	}
	a.region.Store(region)

	a.Allocate(100)
	_ = a.CopyFreeList()

	f.Close()
	os.Remove(f.Name())

	_, _, err = a.Allocate(8 * 1024 * 1024)
	if err == nil {
		t.Fatal("expected error from allocate after file removed")
	}
}

func TestAllocateExactFitFreeBlock(t *testing.T) {
	f, err := os.CreateTemp("", "alloc_exact_*.db")
	if err != nil {
		t.Fatal(err)
	}
	filename := f.Name()
	defer os.Remove(filename)

	f.Truncate(4096)
	a := newAllocator(f)

	off, _, _ := a.Allocate(256)
	a.Free(off, 256)

	off2, len2, err := a.Allocate(256)
	if err != nil {
		t.Fatalf("exact fit allocate failed: %v", err)
	}
	if off2 != off {
		t.Errorf("exact fit offset: got %d, want %d", off2, off)
	}
	if len2 != 256 {
		t.Errorf("exact fit length: got %d, want 256", len2)
	}

	freelist := a.CopyFreeList()
	if len(freelist) != 0 {
		t.Errorf("expected 0 free blocks after exact fit, got %d", len(freelist))
	}

	f.Close()
}
