package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// RawScanSaveRange save range to sst file
func (nemo *NEMO) RawScanSaveRange(path string, start []byte, end []byte, UseSnapshot bool) error {
	var cErr *C.char
	cPath := C.CString(path)
	C.nemo_RawScanSaveAll(nemo.c,
		cPath,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(UseSnapshot),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// IngestFile ingest sst files from path
func (nemo *NEMO) IngestFile(path string) error {
	var cErr *C.char
	cPath := C.CString(path)
	C.nemo_IngestFile(nemo.c, cPath, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}
