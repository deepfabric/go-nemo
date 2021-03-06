package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// LIndex Return the val pointed by the index of a list
func (nemo *NEMO) LIndex(key []byte, index int64) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_LIndex(nemo.c, goByte2char(key), C.size_t(len(key)), C.int64_t(index), &cVal, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.free(unsafe.Pointer(cVal))
	return val, nil
}

// LLen Return list length
func (nemo *NEMO) LLen(key []byte) (int64, error) {
	var cLen C.int64_t
	var cErr *C.char

	C.nemo_LLen(nemo.c, goByte2char(key), C.size_t(len(key)), &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cLen), nil
}

// LPush Push a val into a list
func (nemo *NEMO) LPush(key []byte, value []byte) (int64, error) {
	var cErr *C.char
	var listLen C.int64_t
	C.nemo_LPush(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&listLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(listLen), nil
}

// LPop Pop a val of a list
func (nemo *NEMO) LPop(key []byte) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_LPop(nemo.c, goByte2char(key), C.size_t(len(key)), &cVal, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.free(unsafe.Pointer(cVal))
	return val, nil
}

// LPushx Push a value into a list if the list exists
func (nemo *NEMO) LPushx(key []byte, value []byte) (int64, error) {
	var cErr *C.char
	var listLen C.int64_t
	C.nemo_LPushx(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&listLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(listLen), nil
}

// LRange Return vals between begin and end
func (nemo *NEMO) LRange(key []byte, begin int64, end int64) (index []int64, values [][]byte, err error) {
	var n C.size_t
	var IndexList *C.int64_t
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char
	var cRes C.int64_t

	C.nemo_LRange(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int64_t(begin), C.int64_t(end),
		&n,
		&IndexList, &vallist, &vallistlen,
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, nil, err
	}

	if cRes == 0 || n == 0 {
		return nil, nil, nil
	}

	cIndex := cInt64s2Slice(IndexList, int(n))
	index = make([]int64, int(n))
	for i := range index {
		index[i] = int64(cIndex[i])
	}
	C.free(unsafe.Pointer(IndexList))
	return index, cstr2GoMultiByte(int(n), vallist, vallistlen), nil

}

// LSet Set val of given index
func (nemo *NEMO) LSet(key []byte, index int64, value []byte) error {
	var cErr *C.char
	C.nemo_LSet(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int64_t(index),
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

// LTrim Delete list element between begin and end index
func (nemo *NEMO) LTrim(key []byte, begin int64, end int64) error {
	var cErr *C.char
	C.nemo_LTrim(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int64_t(begin), C.int64_t(end),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// RPush Push element into list in the tail
func (nemo *NEMO) RPush(key []byte, value []byte) (int64, error) {
	var cErr *C.char
	var listLen C.int64_t
	C.nemo_RPush(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&listLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(listLen), nil
}

// RPop Pop a element of the list in the tail
func (nemo *NEMO) RPop(key []byte) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_RPop(nemo.c, goByte2char(key), C.size_t(len(key)), &cVal, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.free(unsafe.Pointer(cVal))
	return val, nil
}

// RPushx Push a element in the tail if the list exists
func (nemo *NEMO) RPushx(key []byte, value []byte) (int64, error) {
	var cErr *C.char
	var listLen C.int64_t
	C.nemo_RPushx(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&listLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(listLen), nil
}

// RPopLPush Pop a element in the src list tail and pushes it into dest list
// If src list is null or does not exist, return false
func (nemo *NEMO) RPopLPush(srckey []byte, destkey []byte) (exist bool, value []byte, err error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char
	var cRes C.int64_t

	C.nemo_RPopLPush(nemo.c,
		goByte2char(srckey), C.size_t(len(srckey)),
		goByte2char(destkey), C.size_t(len(destkey)),
		&cVal, &cLen,
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return false, nil, err
	}

	if cRes == 0 {
		return false, nil, nil
	}

	value = C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.free(unsafe.Pointer(cVal))
	return true, value, nil
}

// LInsert Insert element into list with a pivot and position after pivot
func (nemo *NEMO) LInsert(key []byte, pos int, pivot []byte, value []byte) (int64, error) {
	var cErr *C.char
	var llen C.int64_t
	C.nemo_LInsert(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int(pos),
		goByte2char(pivot), C.size_t(len(pivot)),
		goByte2char(value), C.size_t(len(value)),
		&llen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(llen), nil
}

// LRem Remove element of a list
func (nemo *NEMO) LRem(key []byte, count int64, value []byte) (int64, error) {
	var cErr *C.char
	var resCount C.int64_t
	C.nemo_LRem(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int64_t(count),
		goByte2char(value), C.size_t(len(value)),
		&resCount,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(resCount), nil
}
