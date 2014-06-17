package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type WriteBatch struct {
	db     *DB
	wbatch *leveldb.Batch
}

func (w *WriteBatch) Close() {
}

func (w *WriteBatch) Put(key, value []byte) {
	w.wbatch.Put(key, value)
}

func (w *WriteBatch) Delete(key []byte) {
	w.wbatch.Delete(key)
}

func (w *WriteBatch) Commit() error {
	return w.commit(w.db.writeOpts)
}

func (w *WriteBatch) SyncCommit() error {
	return w.commit(w.db.syncWriteOpts)
}

func (w *WriteBatch) Rollback() {
	w.wbatch.Reset()
}

func (w *WriteBatch) commit(wb *WriteOptions) error {
	return w.db.db.Write(w.wbatch, wb.Opt)
}
