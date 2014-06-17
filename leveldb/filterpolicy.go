package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/filter"
)

type FilterPolicy struct {
	f filter.Filter
}

func NewBloomFilter(bitsPerKey int) *FilterPolicy {
	return &FilterPolicy{filter.NewBloomFilter(bitsPerKey)}
}

func (fp *FilterPolicy) Close() {

}
