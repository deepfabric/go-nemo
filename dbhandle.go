package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"reflect"
	"unsafe"
)

// DBNemo rocksdb instance handle
type DBNemo struct {
	c *C.nemo_DBNemo_t
}

// GetMetaHandle Return db handle of meta data
func (nemo *NEMO) GetMetaHandle() *DBNemo {
	var hd DBNemo
	hd.c = C.nemo_GetMetaHandle(nemo.c)
	return &hd
}

// GetRaftHandle Return db handle of raft log
func (nemo *NEMO) GetRaftHandle() *DBNemo {
	var hd DBNemo
	hd.c = C.nemo_GetRaftHandle(nemo.c)
	return &hd
}

// BatchWrite A batch write api for meta data and raft log rocksdb instance
func (nemo *NEMO) BatchWrite(db *DBNemo, wb *WriteBatch, sync bool) error {
	var cErr *C.char
	C.rocksdb_BatchWrite(nemo.c, db.c, wb.c, C.bool(sync), &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// PutWithHandle Put a key value pair with a db handle
func (nemo *NEMO) PutWithHandle(db *DBNemo, key []byte, value []byte, sync bool) error {
	var cErr *C.char
	C.nemo_PutWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		C.bool(sync),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// GetWithHandle Get a key value pair with a db handle
func (nemo *NEMO) GetWithHandle(db *DBNemo, key []byte) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char
	cCppStr := C.nemo_GetWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)),
		&cVal, &cLen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.nemo_delCppStr(cCppStr)
	return val, nil
}

// GetWithHandleUnSafe Get a key value pair with a db handle
// The second return value is a cpp pointer points to cpp string object.
// Must use func 'FreeCppStr' to free the cpp string object if you don't use the value slice
func (nemo *NEMO) GetWithHandleUnSafe(db *DBNemo, key []byte) ([]byte, unsafe.Pointer, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char
	cCppStr := C.nemo_GetWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)),
		&cVal, &cLen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, nil, res
	}

	var v []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cVal))

	return v, cCppStr, nil
}

// DeleteWithHandle Delete a key value pair with a db handle
func (nemo *NEMO) DeleteWithHandle(db *DBNemo, key []byte, sync bool) error {
	var cErr *C.char
	C.nemo_DeleteWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)), C.bool(sync), &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}
