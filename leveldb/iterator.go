package leveldb

// #cgo LDFLAGS: -lleveldb
// #include <stdlib.h>
// #include "leveldb/c.h"
import "C"

import (
	"bytes"
	"unsafe"
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
	it *C.leveldb_iterator_t

	r *Range

	offset int
	limit  int

	step int

	//0 for IteratorForward, 1 for IteratorBackward
	direction uint8
}

func (it *Iterator) Key() []byte {
	var klen C.size_t
	kdata := C.leveldb_iter_key(it.it, &klen)
	if kdata == nil {
		return nil
	}

	return C.GoBytes(unsafe.Pointer(kdata), C.int(klen))
}

func (it *Iterator) Value() []byte {
	var vlen C.size_t
	vdata := C.leveldb_iter_value(it.it, &vlen)
	if vdata == nil {
		return nil
	}

	return C.GoBytes(unsafe.Pointer(vdata), C.int(vlen))
}

func (it *Iterator) Close() {
	C.leveldb_iter_destroy(it.it)
	it.it = nil
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
			r := bytes.Compare(it.Key(), it.r.Max)
			if it.r.Type&RangeROpen > 0 {
				return !(r >= 0)
			} else {
				return !(r > 0)
			}
		}
	} else {
		if it.r.Min != nil {
			r := bytes.Compare(it.Key(), it.r.Min)
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
	return ucharToBool(C.leveldb_iter_valid(it.it))
}

func (it *Iterator) next() {
	C.leveldb_iter_next(it.it)
}

func (it *Iterator) prev() {
	C.leveldb_iter_prev(it.it)
}

func (it *Iterator) seekToFirst() {
	C.leveldb_iter_seek_to_first(it.it)
}

func (it *Iterator) seekToLast() {
	C.leveldb_iter_seek_to_last(it.it)
}

func (it *Iterator) seek(key []byte) {
	C.leveldb_iter_seek(it.it, (*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)))
}

func newIterator(db *DB, opts *ReadOptions, r *Range, offset int, limit int, direction uint8) *Iterator {
	it := new(Iterator)

	it.it = C.leveldb_create_iterator(db.db, opts.Opt)

	it.r = r
	it.offset = offset
	it.limit = limit
	it.direction = direction

	it.step = 0

	if offset < 0 {
		return it
	}

	if direction == IteratorForward {
		if r.Min == nil {
			it.seekToFirst()
		} else {
			it.seek(r.Min)

			if r.Type&RangeLOpen > 0 {
				if it.valid() && bytes.Equal(it.Key(), r.Min) {
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
				if !bytes.Equal(it.Key(), r.Max) {
					it.prev()
				}
			}

			if r.Type&RangeROpen > 0 {
				if it.valid() && bytes.Equal(it.Key(), r.Max) {
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
