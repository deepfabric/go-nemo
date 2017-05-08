package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

type DBWithTTL struct {
	c *C.nemo_DBWithTTL_t
}

func (nemo *NEMO) GetMetaHandle() *DBWithTTL {
	var hd DBWithTTL
	hd.c = C.nemo_GetMetaHandle(nemo.c)
	return &hd
}

func (nemo *NEMO) GetRaftHandle() *DBWithTTL {
	var hd DBWithTTL
	hd.c = C.nemo_GetRaftHandle(nemo.c)
	return &hd
}

func (nemo *NEMO) BatchWrite(db *DBWithTTL, wb *WriteBatch) error {
	var cErr *C.char
	C.rocksdb_BatchWrite(nemo.c, db.c, wb.c, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) PutWithHandle(db *DBWithTTL, key []byte, value []byte) error {
	var cErr *C.char
	C.nemo_PutWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

func (nemo *NEMO) GetWithHandle(db *DBWithTTL, key []byte) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char
	C.nemo_GetWithHandle(nemo.c, db.c, goByte2char(key), C.size_t(len(key)),
		&cVal, &cLen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.free(unsafe.Pointer(cVal))
	return val, nil
}
