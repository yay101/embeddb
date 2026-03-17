package embeddb

import "fmt"

type btreePageStore interface {
	readPage(pageNum uint32) ([]byte, error)
	writePage(pageNum uint32, data []byte) error
	readRaw(offset int64, buf []byte) error
	writeRaw(offset int64, data []byte) error
	ensurePageCapacity(pageCount uint32) error
	sync() error
	close() error
}

type fileBackedBTreePageStore struct {
	idx *BTreeIndex
}

func (s *fileBackedBTreePageStore) readRaw(offset int64, buf []byte) error {
	if s.idx.mmap != nil {
		_, err := s.idx.mmap.ReadAt(buf, offset)
		if err == nil {
			return nil
		}
	}

	if s.idx.file == nil {
		return fmt.Errorf("index file is not open")
	}

	_, err := s.idx.file.ReadAt(buf, offset)
	return err
}

func (s *fileBackedBTreePageStore) writeRaw(offset int64, data []byte) error {
	if s.idx.file == nil {
		return fmt.Errorf("index file is not open")
	}

	_, err := s.idx.file.WriteAt(data, offset)
	return err
}

func (s *fileBackedBTreePageStore) readPage(pageNum uint32) ([]byte, error) {
	buf := make([]byte, BTreePageSize)
	offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
	if err := s.readRaw(offset, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *fileBackedBTreePageStore) writePage(pageNum uint32, data []byte) error {
	if len(data) < BTreePageSize {
		return fmt.Errorf("page write too small: %d", len(data))
	}
	offset := int64(BTreeHeaderLen + (pageNum * BTreePageSize))
	return s.writeRaw(offset, data[:BTreePageSize])
}

func (s *fileBackedBTreePageStore) ensurePageCapacity(pageCount uint32) error {
	if s.idx.file == nil {
		return fmt.Errorf("index file is not open")
	}

	const preAllocPages = 64
	targetPageCount := ((pageCount + preAllocPages - 1) / preAllocPages) * preAllocPages
	targetOffset := int64(BTreeHeaderLen + (targetPageCount * BTreePageSize))

	currentSize, err := s.idx.file.Seek(0, 2)
	if err != nil {
		return err
	}

	if currentSize < targetOffset {
		if err := s.idx.file.Truncate(targetOffset); err != nil {
			return err
		}
		if err := s.idx.remapFile(); err != nil {
			return err
		}
	}

	return nil
}

func (s *fileBackedBTreePageStore) sync() error {
	if s.idx.file == nil {
		return nil
	}
	return s.idx.file.Sync()
}

func (s *fileBackedBTreePageStore) close() error {
	if s.idx.mmap != nil {
		if err := s.idx.mmap.Close(); err != nil {
			return err
		}
		s.idx.mmap = nil
	}

	if s.idx.file != nil {
		err := s.idx.file.Close()
		s.idx.file = nil
		return err
	}

	return nil
}
