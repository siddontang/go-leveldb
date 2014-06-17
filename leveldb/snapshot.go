package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type Snapshot struct {
	s            *leveldb.Snapshot
	readOpts     *ReadOptions
	iteratorOpts *ReadOptions
}

func (s *Snapshot) Close() {
	s.s.Release()
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.s.Get(key, s.readOpts.Opt)
}

func (s *Snapshot) Iterator(min []byte, max []byte, rangeType uint8, offset int, limit int) *Iterator {
	return newIterator(s.s.NewIterator(nil, s.iteratorOpts.Opt), &Range{min, max, rangeType}, offset, limit, IteratorForward)
}

func (s *Snapshot) RevIterator(min []byte, max []byte, rangeType uint8, offset int, limit int) *Iterator {
	return newIterator(s.s.NewIterator(nil, s.iteratorOpts.Opt), &Range{min, max, rangeType}, offset, limit, IteratorBackward)
}
