package leveldb

import (
	"reflect"
	"unsafe"
)

func slice(p unsafe.Pointer, n int) []byte {
	var b []byte
	pbyte := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pbyte.Data = uintptr(p)
	pbyte.Len = n
	pbyte.Cap = n
	return b
}
