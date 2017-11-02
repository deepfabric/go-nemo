package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"reflect"
	"unsafe"
)

// KIterator is kv db iterator
type KIterator struct {
	c *C.nemo_KIteratorRO_t
}

// HmetaIterator is hash db meta info iterator
type HmetaIterator struct {
	c *C.nemo_HmetaIterator_t
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

// Key Return the key entry, just valid at current iterator cursor
func (it *KIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	cRes = C.KROkey(it.c, &cLen)

	var k []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cRes))
	return k
}

// PooledKey Return the key entry locates at memory pool,
// must return it to memory pool if you don't use it anymore
func (it *KIterator) PooledKey() []byte {
	var cRes *C.char
	var cLen C.size_t
	cRes = C.KROkey(it.c, &cLen)

	var k []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cRes))

	buf := MemPool.Alloc(int(cLen))
	copy(buf, k)
	return buf
}

// Value Return the value entry, just valid at current iterator cursor
func (it *KIterator) Value() []byte {
	var cRes *C.char
	var cLen C.size_t

	cRes = C.KROvalue(it.c, &cLen)

	var v []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cRes))
	return v
}

// PooledValue Return the value entry locates at memory pool,
// must return it to memory pool if you don't use it anymore
func (it *KIterator) PooledValue() []byte {
	var cRes *C.char
	var cLen C.size_t

	cRes = C.KROvalue(it.c, &cLen)

	var v []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cRes))

	buf := MemPool.Alloc(int(cLen))
	copy(buf, v)
	return buf
}

// Free Release the iterator
func (it *KIterator) Free() {
	C.KROIteratorFree(it.c)
}

// HmeataScan return a kv iterator
func (nemo *NEMO) HmeataScan(start []byte, end []byte, UseSnapshot bool, SikpNilIndex bool) *HmetaIterator {
	var hit HmetaIterator
	hit.c = C.nemo_HmetaScan(nemo.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.bool(UseSnapshot), C.bool(SikpNilIndex),
	)
	return &hit
}

// Next Move the iterator to the next element
func (it *HmetaIterator) Next() {
	C.HmetaNext(it.c)
}

// Valid Return true if the iterator is valid
func (it *HmetaIterator) Valid() bool {
	return bool(C.HmetaValid(it.c))
}

// Key Return the key entry, just valid at current iterator cursor
func (it *HmetaIterator) Key() []byte {
	var cRes *C.char
	var cLen C.size_t

	cRes = C.HmetaKey(it.c, &cLen)

	var k []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cRes))
	return k
}

// PooledKey Return the key entry locates at memory pool,
// must return it to memory pool if you don't use it anymore
func (it *HmetaIterator) PooledKey() []byte {
	var cRes *C.char
	var cLen C.size_t
	cRes = C.HmetaKey(it.c, &cLen)

	var k []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cRes))

	buf := MemPool.Alloc(int(cLen))
	copy(buf, k)
	return buf
}

// IndexInfo Return the IndexInfo, just valid at current iterator cursor
func (it *HmetaIterator) IndexInfo() []byte {
	var cRes *C.char
	var cLen C.size_t

	cRes = C.HmetaIndexInfo(it.c, &cLen)

	var index []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&index))
	sH.Cap, sH.Len, sH.Data = int(cLen), int(cLen), uintptr(unsafe.Pointer(cRes))
	return index
}

// Free Release the iterator
func (it *HmetaIterator) Free() {
	C.HmetaIteratorFree(it.c)
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

	cRes = C.Volkey(it.c, &cLen)
	res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
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
