package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// KIterator is kv db iterator
type KIterator struct {
	c *C.nemo_KIteratorRO_t
}

// VolumeIterator is a iterator for five type data volume
type VolumeIterator struct {
	c *C.nemo_VolumeIterator_t
}

// KScanWithHandle Return a kv iterator
func (nemo *NEMO) KScanWithHandle(db *DBNemo, start []byte, end []byte, UseSnapshot bool) *KIterator {
	var kit KIterator
	kit.c = C.nemo_KScanWithHandle(nemo.c, db.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(UseSnapshot),
	)
	return &kit
}

// SeekWithHandle Return next key value pair which is not less than start key
func (nemo *NEMO) SeekWithHandle(db *DBNemo, start []byte) ([]byte, []byte, error) {
	var cKey *C.char
	var cKeyLen C.size_t
	var cVal *C.char
	var cValLen C.size_t
	var cErr *C.char
	var nKey []byte
	var nVal []byte

	C.nemo_SeekWithHandle(nemo.c, db.c,
		goByte2char(start), C.size_t(len(start)),
		&cKey, &cKeyLen,
		&cVal, &cValLen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, nil, res
	}
	if cKeyLen != 0 {
		nKey = C.GoBytes(unsafe.Pointer(cKey), C.int(cKeyLen))
		C.free(unsafe.Pointer(cKey))
	} else {
		nKey = nil
	}
	if cValLen != 0 {
		nVal = C.GoBytes(unsafe.Pointer(cVal), C.int(cValLen))
		C.free(unsafe.Pointer(cVal))
	} else {
		nVal = nil
	}

	return nKey, nVal, nil

}

// Next Move the iterator to the next element
func (it *KIterator) Next() {
	C.KRONext(it.c)
}

// Valid Return true if the iterator is valid
func (it *KIterator) Valid() bool {
	return bool(C.KROValid(it.c))
}

// Key Return the key entry of the iterator
func (it *KIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.KROkey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

// Value Return the value entry of the iterator
func (it *KIterator) Value() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.KROvalue(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

// Free Release the iterator
func (it *KIterator) Free() {
	C.KROIteratorFree(it.c)
}

// NewVolumeIterator Return the volume iterator
func (nemo *NEMO) NewVolumeIterator(start []byte, end []byte) *VolumeIterator {
	var it VolumeIterator
	it.c = C.createVolumeIterator(nemo.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(false),
	)
	return &it
}

// Next Move the iterator to the next element
func (it *VolumeIterator) Next() {
	C.VolNext(it.c)
}

// Valid Return true if the iterator is valid
func (it *VolumeIterator) Valid() bool {
	return bool(C.VolValid(it.c))
}

// Key Return the key entry of the iterator
func (it *VolumeIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.Volkey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

// Value Return the value entry of the iterator
func (it *VolumeIterator) Value() int64 {
	var cRes C.int64_t
	C.Volvalue(it.c, &cRes)
	return int64(cRes)
}

// TargetScan find whether range volume is large than input target volume
// call it immediately after new a VolumeIterator
// must not call this func after iterator Next()!
func (it *VolumeIterator) TargetScan(target int64) bool {
	return bool(C.VoltargetScan(it.c, C.int64_t(target)))
}

// TargetKey Return the target key if targetScan return true otherwise return nil
func (it *VolumeIterator) TargetKey() []byte {
	var cRes *C.char
	var cLen C.size_t

	C.VoltargetKey(it.c, &cRes, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return res
}

// TotalVolume return current total volume if targetScan return false,otherwise return 0
func (it *VolumeIterator) TotalVolume() int64 {
	return int64(C.VoltotalVolume(it.c))
}

// Free Release the iterator
func (it *VolumeIterator) Free() {
	C.VolIteratorFree(it.c)
}
