package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// HSet Set a field with value on hash table named as key
// Return 1 if the field does not exist,otherwise return 0
func (nemo *NEMO) HSet(key []byte, field []byte, value []byte) (int, error) {
	var cErr *C.char
	var iExist C.int
	C.nemo_HSet(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(field), C.size_t(len(field)),
		goByte2char(value), C.size_t(len(value)),
		&iExist,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int(iExist), nil
}

// HGet Get a value of a field in hash table named as key
func (nemo *NEMO) HGet(key []byte, field []byte) ([]byte, error) {
	var cErr *C.char
	var cVal *C.char
	var cLen C.size_t
	C.nemo_HGet(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(field), C.size_t(len(field)),
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

// HDel Delete muli fileds in hash table named as key
// Return the count of deleted keys which don't contain unexisted keys
func (nemo *NEMO) HDel(key []byte, fields ...[]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t

	l := len(fields)

	cfieldlist := make([]*C.char, l)
	cfieldlen := make([]C.size_t, l)

	for i, field := range fields {
		cfieldlist[i] = goBytedup2char(field)
		cfieldlen[i] = C.size_t(len(field))
	}

	C.nemo_HMDel(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&cfieldlist[0])),
		(*C.size_t)(unsafe.Pointer(&cfieldlen[0])),
		&cRes,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

// HExists Return true if the field has been set in the hash table named as key
func (nemo *NEMO) HExists(key []byte, field []byte) (bool, error) {
	var cIfExist C.bool
	var cErr *C.char
	C.nemo_HExists(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(field), C.size_t(len(field)),
		&cIfExist, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return false, res
	}
	return bool(cIfExist), nil
}

// HKeys Return all fields in the hash table named as key
func (nemo *NEMO) HKeys(key []byte) ([][]byte, error) {
	var n C.int
	var fieldlist **C.char
	var fieldlistlen *C.size_t
	var cErr *C.char
	C.nemo_HKeys(nemo.c, goByte2char(key), C.size_t(len(key)), &n, &fieldlist, &fieldlistlen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	}
	return cstr2GoMultiByte(int(n), fieldlist, fieldlistlen), nil

}

// HVals Returns all values int the hash table named as key
func (nemo *NEMO) HVals(key []byte) ([][]byte, error) {
	var n C.int
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char
	C.nemo_HVals(nemo.c, goByte2char(key), C.size_t(len(key)), &vallist, &vallistlen, &n, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	}
	return cstr2GoMultiByte(int(n), vallist, vallistlen), nil

}

// HGetall Return all field,value pairs in the hash table named as key
func (nemo *NEMO) HGetall(key []byte) ([][]byte, [][]byte, error) {
	var n C.int
	var fieldlist **C.char
	var fieldlistlen *C.size_t
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char
	C.nemo_HGetall(nemo.c, goByte2char(key), C.size_t(len(key)), &n, &fieldlist, &fieldlistlen, &vallist, &vallistlen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, nil, res
	}

	if n == 0 {
		return nil, nil, nil
	}
	return cstr2GoMultiByte(int(n), fieldlist, fieldlistlen), cstr2GoMultiByte(int(n), vallist, vallistlen), nil
}

// HLen Return the element count of a hash table
func (nemo *NEMO) HLen(key []byte) (int64, error) {
	var cLen C.int64_t
	var cErr *C.char
	C.nemo_HLen(nemo.c, goByte2char(key), C.size_t(len(key)), &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cLen), nil
}

// HMGet Get multi values of multi fields in a hash table
func (nemo *NEMO) HMGet(key []byte, fields [][]byte) ([][]byte, []error) {
	l := len(fields)
	cfieldlist := make([]*C.char, l)
	cfieldlen := make([]C.size_t, l)
	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)
	cErrs := make([]*C.char, l)
	errs := make([]error, l)
	var ErrOK *C.char
	for i, filed := range fields {
		cfieldlist[i] = goBytedup2char(filed)
		cfieldlen[i] = C.size_t(len(filed))
	}
	C.nemo_HMGet(nemo.c, goByte2char(key), C.size_t(len(key)), C.int(l),
		(**C.char)(unsafe.Pointer(&cfieldlist[0])),
		(*C.size_t)(unsafe.Pointer(&cfieldlen[0])),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		(**C.char)(unsafe.Pointer(&cErrs[0])),
		&ErrOK,
	)
	for _, field := range cfieldlist {
		C.free(unsafe.Pointer(field))
	}

	for i, cerr := range cErrs {
		if cerr == nil {
			errs[i] = nil
		} else {
			errs[i] = errors.New(C.GoString(cerr))
			C.free(unsafe.Pointer(cerr))
		}
	}
	return cSlice2MultiByte(l, cvallist, cvallen), errs
}

// HMSet Set multi fields with multi values in a hash table
// Return every result of every single set like HSet
func (nemo *NEMO) HMSet(key []byte, fields [][]byte, vals [][]byte) ([]int, error) {
	var cErr *C.char
	l := len(fields)
	if len(vals) != l {
		return nil, errors.New("key len != val len")
	}
	cfieldlist := make([]*C.char, l)
	cfieldlen := make([]C.size_t, l)
	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)
	creslist := make([]C.int, l)
	goreslist := make([]int, l)

	for i, field := range fields {
		cfieldlist[i] = goBytedup2char(field)
		cfieldlen[i] = C.size_t(len(field))
	}
	for i, val := range vals {
		cvallist[i] = goBytedup2char(val)
		cvallen[i] = C.size_t(len(val))
	}

	C.nemo_HMSet(nemo.c, goByte2char(key), C.size_t(len(key)), C.int(l),
		(**C.char)(unsafe.Pointer(&cfieldlist[0])),
		(*C.size_t)(unsafe.Pointer(&cfieldlen[0])),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		(*C.int)(unsafe.Pointer(&creslist[0])),
		&cErr,
	)

	for _, field := range cfieldlist {
		C.free(unsafe.Pointer(field))
	}
	for _, val := range cvallist {
		C.free(unsafe.Pointer(val))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	for i := range goreslist {
		goreslist[i] = int(creslist[i])
	}

	return goreslist, nil
}

// HSetnx Set an existed field with value in a hash table
// Return 1 if the field does not exist, otherwise 0
func (nemo *NEMO) HSetnx(key []byte, field []byte, value []byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	C.nemo_HSetnx(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(field), C.size_t(len(field)),
		goByte2char(value), C.size_t(len(value)),
		&cRes, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

// HStrlen Return the value length of a filed in a hash table
func (nemo *NEMO) HStrlen(key []byte, field []byte) (int64, error) {
	var cLen C.int64_t
	var cErr *C.char
	C.nemo_HStrlen(nemo.c, goByte2char(key), C.size_t(len(key)),
		goByte2char(field), C.size_t(len(field)),
		&cLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cLen), nil
}

// HIncrby Increment a filed by a integer
// If origin key or member does not exist,new key will be set as "by"
// If origin member is not a integer, return an error
func (nemo *NEMO) HIncrby(key []byte, field []byte, by int64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_HIncrby(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(field), C.size_t(len(field)),
		C.int64_t(by), &cRes, &cLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	Res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return Res, nil
}

//nemo_HIncrbyfloat
