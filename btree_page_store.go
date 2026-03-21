package embeddb

type btreePageStore interface {
    readPageInto(pageNum uint32, buf []byte) error
    writePage(pageNum uint32, data []byte) error
    readRaw(offset int64, buf []byte) error
    writeRaw(offset int64, data []byte) error
    ensurePageCapacity(pageCount uint32) error
    sync() error
    close() error
}
