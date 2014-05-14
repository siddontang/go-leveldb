package leveldb

// #cgo LDFLAGS: -lleveldb
// #include <stdint.h>
// #include "leveldb/c.h"
import "C"

type Snapshot struct {
	db *DB

	snap *C.leveldb_snapshot_t

	readOpts     *ReadOptions
	iteratorOpts *ReadOptions
}

func (s *Snapshot) Close() {
	C.leveldb_release_snapshot(s.db.db, s.snap)

	s.iteratorOpts.Close()
	s.readOpts.Close()
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.db.get(s.readOpts, key)
}

func (s *Snapshot) Iterator(min []byte, max []byte, rangeType uint8, offset int, limit int) *Iterator {
	return newIterator(s.db, s.iteratorOpts, &Range{min, max, rangeType}, offset, limit, IteratorForward)
}

func (s *Snapshot) RevIterator(min []byte, max []byte, rangeType uint8, offset int, limit int) *Iterator {
	return newIterator(s.db, s.iteratorOpts, &Range{min, max, rangeType}, offset, limit, IteratorBackward)
}
