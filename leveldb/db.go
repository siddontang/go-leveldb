package leveldb

import (
	"encoding/json"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
)

var (
	ErrNotImplemented = errors.New("function not implemented")
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

	db *leveldb.DB

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
	if err := os.MkdirAll(cfg.Path, os.ModePerm); err != nil {
		return nil, err
	}

	db := new(DB)
	db.cfg = cfg

	if err := db.open(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) open() error {
	db.opts = db.initOptions(db.cfg)

	db.readOpts = NewReadOptions()
	db.writeOpts = NewWriteOptions()

	db.iteratorOpts = NewReadOptions()
	db.iteratorOpts.SetFillCache(false)

	db.syncWriteOpts = NewWriteOptions()
	db.syncWriteOpts.SetSync(true)

	var err error
	db.db, err = leveldb.OpenFile(db.cfg.Path, db.opts.Opt)

	return err
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
	db.db.Close()

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
}

func (db *DB) Destroy() error {
	path := db.cfg.Path

	db.Close()

	return os.RemoveAll(path)
}

func (db *DB) Clear() error {
	bc := db.NewWriteBatch()
	defer bc.Close()

	var err error
	it := db.Iterator(nil, nil, RangeClose, 0, -1)
	num := 0
	for ; it.Valid(); it.Next() {
		bc.Delete(it.Key())
		num++
		if num == 1000 {
			num = 0
			if err = bc.Commit(); err != nil {
				return err
			}
		}
	}

	err = bc.Commit()

	return err
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
		wbatch: new(leveldb.Batch),
	}
	return wb
}

func (db *DB) NewSnapshot() *Snapshot {
	snap, _ := db.db.GetSnapshot()
	s := &Snapshot{
		s:            snap,
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
	return newIterator(db.db.NewIterator(nil, db.iteratorOpts.Opt), &Range{min, max, rangeType}, offset, limit, IteratorForward)
}

//limit < 0, unlimit
//offset must >= 0, if < 0, will get nothing
func (db *DB) RevIterator(min []byte, max []byte, rangeType uint8, offset int, limit int) *Iterator {
	return newIterator(db.db.NewIterator(nil, db.iteratorOpts.Opt), &Range{min, max, rangeType}, offset, limit, IteratorBackward)
}

func (db *DB) put(wo *WriteOptions, key, value []byte) error {
	return db.db.Put(key, value, wo.Opt)
}

func (db *DB) get(ro *ReadOptions, key []byte) ([]byte, error) {
	v, err := db.db.Get(key, ro.Opt)
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else {
		return v, err
	}
}

func (db *DB) delete(wo *WriteOptions, key []byte) error {
	return db.db.Delete(key, wo.Opt)
}
