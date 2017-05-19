package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"unsafe"
)

type KIterator struct {
	c *C.nemo_KIterator_t
}
type RawIterator struct {
	c *C.nemo_RawIterator_t
}
type VolumeIterator struct {
	c *C.nemo_VolumeIterator_t
}

func (nemo *NEMO) KScan(start []byte, end []byte, limit int64) *KIterator {
	var it KIterator
	it.c = C.nemo_KScan(nemo.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.uint64_t(limit), C.bool(false),
	)
	return &it
}

func (it *KIterator) Next() {
	C.KNext(it.c)
}

func (it *KIterator) Valid() bool {
	return bool(C.KValid(it.c))
}

func (it *KIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.Kkey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *KIterator) Value() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.Kvalue(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *KIterator) Free() {
	C.KIteratorFree(it.c)
}

func (nemo *NEMO) RawScanWithHanlde(db *DBNemo, use_snapshot bool) *RawIterator {
	var it RawIterator
	it.c = C.nemo_RawScanWithHandle(nemo.c, db.c, C.bool(false))
	return &it
}

func (it *RawIterator) Next() {
	C.RawNext(it.c)
}

func (it *RawIterator) Valid() bool {
	return bool(C.RawValid(it.c))
}

func (it *RawIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.RawKey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *RawIterator) Value() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.RawValue(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *RawIterator) Seek(key []byte) {
	C.RawSeek(it.c, goByte2char(key), C.size_t(len(key)))
}

func (it *RawIterator) Free() {
	C.RawIteratorFree(it.c)
}

func (nemo *NEMO) NewVolumeIterator(start []byte, end []byte) *VolumeIterator {
	var it VolumeIterator
	it.c = C.createVolumeIterator(nemo.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(false),
	)
	return &it
}

func (it *VolumeIterator) Next() {
	C.VolNext(it.c)
}

func (it *VolumeIterator) Valid() bool {
	return bool(C.VolValid(it.c))
}

func (it *VolumeIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.Volkey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

func (it *VolumeIterator) Value() int64 {
	var cRes C.int64_t
	C.Volvalue(it.c, &cRes)
	return int64(cRes)
}

func (it *VolumeIterator) Free() {
	C.VolIteratorFree(it.c)
}
