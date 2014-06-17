package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb/cache"
)

type Cache struct {
	c cache.Cache
}

func NewLRUCache(capacity int) *Cache {
	return &Cache{cache.NewLRUCache(capacity)}
}

func (c *Cache) Close() {

}
