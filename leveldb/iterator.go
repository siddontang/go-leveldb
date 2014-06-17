package leveldb

import (
	"bytes"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	IteratorForward  uint8 = 0
	IteratorBackward uint8 = 1
)

const (
	RangeClose uint8 = 0x00
	RangeLOpen uint8 = 0x01
	RangeROpen uint8 = 0x10
	RangeOpen  uint8 = 0x11
)

//min must less or equal than max
//range type:
//close: [min, max]
//open: (min, max)
//lopen: (min, max]
//ropen: [min, max)
type Range struct {
	Min []byte
	Max []byte

	Type uint8
}

type Iterator struct {
	it iterator.Iterator

	r *Range

	offset int
	limit  int

	step int

	//0 for IteratorForward, 1 for IteratorBackward
	direction uint8

	itValid bool
}

func (it *Iterator) Key() []byte {
	return append([]byte{}, it.it.Key()...)
}

func (it *Iterator) Value() []byte {
	return append([]byte{}, it.it.Value()...)
}

func (it *Iterator) Close() {
	it.it.Release()
}

func (it *Iterator) Valid() bool {
	if it.offset < 0 {
		return false
	} else if !it.valid() {
		return false
	} else if it.limit >= 0 && it.step >= it.limit {
		return false
	}

	if it.direction == IteratorForward {
		if it.r.Max != nil {
			r := bytes.Compare(it.it.Key(), it.r.Max)
			if it.r.Type&RangeROpen > 0 {
				return !(r >= 0)
			} else {
				return !(r > 0)
			}
		}
	} else {
		if it.r.Min != nil {
			r := bytes.Compare(it.it.Key(), it.r.Min)
			if it.r.Type&RangeLOpen > 0 {
				return !(r <= 0)
			} else {
				return !(r < 0)
			}
		}
	}

	return true
}

func (it *Iterator) Next() {
	it.step++

	if it.direction == IteratorForward {
		it.next()
	} else {
		it.prev()
	}
}

func (it *Iterator) valid() bool {
	return it.itValid
}

func (it *Iterator) next() {
	it.itValid = it.it.Next()
}

func (it *Iterator) prev() {
	it.itValid = it.it.Prev()
}

func (it *Iterator) seekToFirst() {
	it.itValid = it.it.First()
}

func (it *Iterator) seekToLast() {
	it.itValid = it.it.Last()
}

func (it *Iterator) seek(key []byte) {
	it.itValid = it.it.Seek(key)
}

func newIterator(i iterator.Iterator, r *Range, offset int, limit int, direction uint8) *Iterator {
	it := new(Iterator)

	it.it = i

	it.r = r
	it.offset = offset
	it.limit = limit
	it.direction = direction

	it.step = 0

	it.itValid = true

	if offset < 0 {
		return it
	}

	if direction == IteratorForward {
		if r.Min == nil {
			it.seekToFirst()
		} else {
			it.seek(r.Min)

			if r.Type&RangeLOpen > 0 {
				if it.valid() && bytes.Equal(it.it.Key(), r.Min) {
					it.next()
				}
			}
		}
	} else {
		if r.Max == nil {
			it.seekToLast()
		} else {
			it.seek(r.Max)

			if !it.valid() {
				it.seekToLast()
			} else {
				if !bytes.Equal(it.it.Key(), r.Max) {
					it.prev()
				}
			}

			if r.Type&RangeROpen > 0 {
				if it.valid() && bytes.Equal(it.it.Key(), r.Max) {
					it.prev()
				}
			}
		}
	}

	for i := 0; i < offset; i++ {
		if it.valid() {
			if it.direction == IteratorForward {
				it.next()
			} else {
				it.prev()
			}
		}
	}

	return it
}
