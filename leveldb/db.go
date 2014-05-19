package leveldb

/*
#cgo LDFLAGS: -lleveldb
#include <leveldb/c.h>
*/
import "C"

import (
	"encoding/json"
	"unsafe"
)

const defaultFilterBits int = 10

type Config struct {
	Path string `json:"path"`

	Compression bool `json:"compression"`

	BlockSize       int `json:"block_size"`
	WriteBufferSize int `json:"write_buffer_size"`
	CacheSize       int `json:"cache_size"`
}

type DB struct {
	cfg *Config

	db *C.leveldb_t

	opts *Options

	//for default read and write options
	readOpts     *ReadOptions
	writeOpts    *WriteOptions
	iteratorOpts *ReadOptions

	syncWriteOpts *WriteOptions

	cache *Cache

	filter *FilterPolicy
}

func Open(configJson json.RawMessage) (*DB, error) {
	cfg := new(Config)
	err := json.Unmarshal(configJson, cfg)
	if err != nil {
		return nil, err
	}

	return OpenWithConfig(cfg)
}

func OpenWithConfig(cfg *Config) (*DB, error) {
	db := new(DB)
	db.cfg = cfg

	db.opts = db.initOptions(cfg)

	db.readOpts = NewReadOptions()
	db.writeOpts = NewWriteOptions()

	db.iteratorOpts = NewReadOptions()
	db.iteratorOpts.SetFillCache(false)

	db.syncWriteOpts = NewWriteOptions()
	db.syncWriteOpts.SetSync(true)

	var errStr *C.char
	ldbname := C.CString(cfg.Path)
	defer C.leveldb_free(unsafe.Pointer(ldbname))

	db.db = C.leveldb_open(db.opts.Opt, ldbname, &errStr)
	if errStr != nil {
		return nil, saveError(errStr)
	}

	return db, nil
}

func (db *DB) initOptions(cfg *Config) *Options {
	opts := NewOptions()

	opts.SetCreateIfMissing(true)

	if cfg.CacheSize > 0 {
		db.cache = NewLRUCache(cfg.CacheSize)
		opts.SetCache(db.cache)
	}

	//we must use bloomfilter
	db.filter = NewBloomFilter(defaultFilterBits)
	opts.SetFilterPolicy(db.filter)

	if !cfg.Compression {
		opts.SetCompression(NoCompression)
	}

	if cfg.BlockSize > 0 {
		opts.SetBlockSize(cfg.BlockSize)
	}

	if cfg.WriteBufferSize > 0 {
		opts.SetWriteBufferSize(cfg.WriteBufferSize)
	}

	return opts
}

func (db *DB) Close() {
	db.opts.Close()

	if db.cache != nil {
		db.cache.Close()
	}

	if db.filter != nil {
		db.filter.Close()
	}

	db.readOpts.Close()
	db.writeOpts.Close()
	db.iteratorOpts.Close()
	db.syncWriteOpts.Close()

	C.leveldb_close(db.db)
	db.db = nil
}

func (db *DB) Destroy() error {
	path := db.cfg.Path

	db.Close()

	opts := NewOptions()
	defer opts.Close()

	var errStr *C.char
	ldbname := C.CString(path)
	defer C.leveldb_free(unsafe.Pointer(ldbname))

	C.leveldb_destroy_db(opts.Opt, ldbname, &errStr)
	if errStr != nil {
		return saveError(errStr)
	}
	return nil
}

func (db *DB) Clear() {
	it := db.Iterator(nil, nil, 0, 0, -1)
	for ; it.Valid(); it.Next() {
		db.Delete(it.Key())
	}
}

func (db *DB) Put(key, value []byte) error {
	return db.put(db.writeOpts, key, value)
}

func (db *DB) SyncPut(key, value []byte) error {
	return db.put(db.syncWriteOpts, key, value)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.get(db.readOpts, key)
}

func (db *DB) Delete(key []byte) error {
	return db.delete(db.writeOpts, key)
}

func (db *DB) SyncDelete(key []byte) error {
	return db.delete(db.syncWriteOpts, key)
}

func (db *DB) NewWriteBatch() *WriteBatch {
	wb := &WriteBatch{
		db:     db,
		wbatch: C.leveldb_writebatch_create(),
	}
	return wb
}

func (db *DB) NewSnapshot() *Snapshot {
	s := &Snapshot{
		db:           db,
		snap:         C.leveldb_create_snapshot(db.db),
		readOpts:     NewReadOptions(),
		iteratorOpts: NewReadOptions(),
	}

	s.readOpts.SetSnapshot(s)
	s.iteratorOpts.SetSnapshot(s)
	s.iteratorOpts.SetFillCache(false)

	return s
}

//limit < 0, unlimit
//offset must >= 0, if < 0, will get nothing
func (db *DB) Iterator(min []byte, max []byte, rangeType uint8, offset int, limit int) *Iterator {
	return newIterator(db, db.iteratorOpts, &Range{min, max, rangeType}, offset, limit, IteratorForward)
}

//limit < 0, unlimit
//offset must >= 0, if < 0, will get nothing
func (db *DB) RevIterator(min []byte, max []byte, rangeType uint8, offset int, limit int) *Iterator {
	return newIterator(db, db.iteratorOpts, &Range{min, max, rangeType}, offset, limit, IteratorBackward)
}

func (db *DB) put(wo *WriteOptions, key, value []byte) error {
	var errStr *C.char
	var k, v *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if len(value) != 0 {
		v = (*C.char)(unsafe.Pointer(&value[0]))
	}

	lenk := len(key)
	lenv := len(value)
	C.leveldb_put(
		db.db, wo.Opt, k, C.size_t(lenk), v, C.size_t(lenv), &errStr)

	if errStr != nil {
		return saveError(errStr)
	}
	return nil
}

func (db *DB) get(ro *ReadOptions, key []byte) ([]byte, error) {
	var errStr *C.char
	var vallen C.size_t
	var k *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}

	value := C.leveldb_get(
		db.db, ro.Opt, k, C.size_t(len(key)), &vallen, &errStr)

	if errStr != nil {
		return nil, saveError(errStr)
	}

	if value == nil {
		return nil, nil
	}

	defer C.leveldb_free(unsafe.Pointer(value))
	return C.GoBytes(unsafe.Pointer(value), C.int(vallen)), nil
}

func (db *DB) delete(wo *WriteOptions, key []byte) error {
	var errStr *C.char
	var k *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}

	C.leveldb_delete(
		db.db, wo.Opt, k, C.size_t(len(key)), &errStr)

	if errStr != nil {
		return saveError(errStr)
	}
	return nil
}
