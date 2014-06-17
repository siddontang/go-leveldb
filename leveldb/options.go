package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type CompressionOpt int

const (
	NoCompression     = CompressionOpt(opt.NoCompression)
	SnappyCompression = CompressionOpt(opt.SnappyCompression)
)

type Options struct {
	Opt *opt.Options
}

type ReadOptions struct {
	Opt *opt.ReadOptions
}

type WriteOptions struct {
	Opt *opt.WriteOptions
}

func NewOptions() *Options {
	return &Options{&opt.Options{}}
}

func NewReadOptions() *ReadOptions {
	return &ReadOptions{&opt.ReadOptions{}}
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{&opt.WriteOptions{}}
}

func (o *Options) Close() {
}

func (o *Options) SetErrorIfExists(error_if_exists bool) {
	o.Opt.ErrorIfExist = error_if_exists
}

func (o *Options) SetCache(cache *Cache) {
	o.Opt.BlockCache = cache.c
}

func (o *Options) SetWriteBufferSize(s int) {
	o.Opt.WriteBuffer = s
}

func (o *Options) SetParanoidChecks(pc bool) {

}

func (o *Options) SetMaxOpenFiles(n int) {
	o.Opt.MaxOpenFiles = n
}

func (o *Options) SetBlockSize(s int) {
	o.Opt.BlockSize = s
}

func (o *Options) SetBlockRestartInterval(n int) {
	o.Opt.BlockRestartInterval = n
}

func (o *Options) SetCompression(t CompressionOpt) {
	o.Opt.Compression = opt.Compression(t)
}

func (o *Options) SetCreateIfMissing(b bool) {
	o.Opt.ErrorIfMissing = !b
}

func (o *Options) SetFilterPolicy(fp *FilterPolicy) {
	o.Opt.Filter = fp.f
}

func (ro *ReadOptions) Close() {
}

func (ro *ReadOptions) SetVerifyChecksums(b bool) {
}

func (ro *ReadOptions) SetFillCache(b bool) {
	ro.Opt.DontFillCache = !b
}

func (ro *ReadOptions) SetSnapshot(snap *Snapshot) {

}

func (wo *WriteOptions) Close() {

}

func (wo *WriteOptions) SetSync(b bool) {
	wo.Opt.Sync = b
}
