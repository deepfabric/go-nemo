package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// RangeDel Delete key between range
func (nemo *NEMO) RangeDel(start []byte, end []byte) error {
	var cErr *C.char
	C.nemo_RangeDel(nemo.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// RangeDelWithHandle Delete key between range
func (nemo *NEMO) RangeDelWithHandle(db *DBNemo, start []byte, end []byte) error {
	var cErr *C.char
	C.nemo_RangeDelWithHandle(nemo.c, db.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}
