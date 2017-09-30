package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"reflect"
	"unsafe"
)

// Get Return value of a key
func (nemo *NEMO) Get(key []byte) ([]byte, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char

	cCppStr := C.nemo_Get(nemo.c, goByte2char(key), C.size_t(len(key)), &cVal, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		C.nemo_delCppStr(cCppStr)
		return nil, res
	}
	val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
	C.nemo_delCppStr(cCppStr)
	return val, nil
}

// GetUnSafe Return a byte slice which contains value of a key using no memory copy,
// The second return value is a cpp pointer points to cpp string object.
// Must use func 'FreeCppStr' to free the cpp string object if you don't use the value slice
func (nemo *NEMO) GetUnSafe(key []byte) ([]byte, unsafe.Pointer, error) {
	var cVal *C.char
	var cLen C.size_t
	var cErr *C.char

	cCppStr := C.nemo_Get(nemo.c, goByte2char(key), C.size_t(len(key)), &cVal, &cLen, &cErr)
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

// Set Set a key by value
func (nemo *NEMO) Set(key []byte, value []byte, ttl int) error {
	var (
		cErr *C.char
	)
	C.nemo_Set(nemo.c, goByte2char(key), C.size_t(len(key)), goByte2char(value), C.size_t(len(value)), C.int32_t(ttl), &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// MGet Get multi values of muli keys
func (nemo *NEMO) MGet(keys [][]byte) ([][]byte, []error) {
	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)
	cErrs := make([]*C.char, l)
	errs := make([]error, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}
	vp := C.nemo_MGet(nemo.c, C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		(**C.char)(unsafe.Pointer(&cErrs[0])),
	)
	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	for i, cerr := range cErrs {
		if cerr == nil {
			errs[i] = nil
		} else {
			errs[i] = errors.New(C.GoString(cerr))
			C.free(unsafe.Pointer(cerr))
		}
	}

	res := cArray2goMSliceObj(l, cvallist, cvallen)
	FreeCppSSVector(vp)
	return res, errs
}

// MGetUnSafe Get multi values of muli keys
// The second return value is the cpp vector pointer
// Must use func 'FreeCppSSVector' to free the cpp vector object if you don't use the value slice array
func (nemo *NEMO) MGetUnSafe(keys [][]byte) ([][]byte, unsafe.Pointer, []error) {
	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)
	cErrs := make([]*C.char, l)
	errs := make([]error, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}
	vp := C.nemo_MGet(nemo.c, C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		(**C.char)(unsafe.Pointer(&cErrs[0])),
	)
	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	for i, cerr := range cErrs {
		if cerr == nil {
			errs[i] = nil
		} else {
			errs[i] = errors.New(C.GoString(cerr))
			C.free(unsafe.Pointer(cerr))
		}
	}

	return cArray2goMSlice(l, cvallist, cvallen), vp, errs
}

// MSet Set muli keys with multi values
func (nemo *NEMO) MSet(keys [][]byte, vals [][]byte) error {
	var cErr *C.char
	l := len(keys)
	if len(vals) != l {
		return errors.New("key len != val len")
	}
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	cvallist := make([]*C.char, l)
	cvallen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}
	for i, val := range vals {
		cvallist[i] = goBytedup2char(val)
		cvallen[i] = C.size_t(len(val))
	}

	C.nemo_MSet(nemo.c, C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		(**C.char)(unsafe.Pointer(&cvallist[0])),
		(*C.size_t)(unsafe.Pointer(&cvallen[0])),
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}
	for _, val := range cvallist {
		C.free(unsafe.Pointer(val))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// Keys Return all keys with specified pattern
func (nemo *NEMO) Keys(pattern []byte) ([][]byte, error) {
	var cPattern *C.char = goByte2char(pattern)
	var cPatternlen C.size_t = C.size_t(len(pattern))
	var n C.int
	var keylist **C.char
	var keylistlen *C.size_t
	var cErr *C.char
	C.nemo_Keys(nemo.c, cPattern, cPatternlen, &n, &keylist, &keylistlen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	}
	return cstr2GoMultiByte(int(n), keylist, keylistlen), nil

}

// Incrby Increment a key by a integer
// If origin key does not exist,new key will be set as "by"
// If origin key is not a integer, return an error
func (nemo *NEMO) Incrby(key []byte, by int64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_Incrby(nemo.c, goByte2char(key), C.size_t(len(key)), C.int64_t(by), &cRes, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	Res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return Res, nil
}

// Decrby Descrement a key by a integer
// If origin key does not exist,new key will be set as "-by"
// If origin key is not a integer, return an error
func (nemo *NEMO) Decrby(key []byte, by int64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_Decrby(nemo.c, goByte2char(key), C.size_t(len(key)), C.int64_t(by), &cRes, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	Res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return Res, nil
}

// IncrbyFloat Increment a key by a float
// If origin key does not exist,new key will be set as "by"
// If origin key is not a float, return an error
func (nemo *NEMO) IncrbyFloat(key []byte, by float64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_Incrbyfloat(nemo.c, goByte2char(key), C.size_t(len(key)), C.double(by), &cRes, &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	Res := C.GoBytes(unsafe.Pointer(cRes), C.int(cLen))
	C.free(unsafe.Pointer(cRes))
	return Res, nil
}

// GetSet Get old value of a key and set new value.
// If key does not exist, return an error.
func (nemo *NEMO) GetSet(key []byte, value []byte, ttl int) ([]byte, error) {
	var (
		cErr       *C.char
		cOldVal    *C.char
		cOldValLen C.size_t
	)
	C.nemo_GetSet(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cOldVal, &cOldValLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	OldVal := C.GoBytes(unsafe.Pointer(cOldVal), C.int(cOldValLen))
	C.free(unsafe.Pointer(cOldVal))
	return OldVal, nil
}

// Append Append a key with value,returns new length
func (nemo *NEMO) Append(key []byte, value []byte) (int64, error) {
	var (
		cErr    *C.char
		cNewLen C.int64_t
	)
	C.nemo_Append(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cNewLen, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cNewLen), nil
}

// Setnx Set a key with value if the key does not exist
// Return 1 if the key does not exist
func (nemo *NEMO) Setnx(key []byte, value []byte, ttl int32) (int64, error) {
	var (
		cErr *C.char
		cRet C.int64_t
	)
	C.nemo_Setnx(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cRet, C.int32_t(ttl), &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRet), nil
}

// Setxx Set a key with value if the key does exist
// return 1 if the key does exist
func (nemo *NEMO) Setxx(key []byte, value []byte, ttl int32) (int64, error) {
	var (
		cErr *C.char
		cRet C.int64_t
	)
	C.nemo_Setxx(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(value), C.size_t(len(value)),
		&cRet, C.int32_t(ttl), &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRet), nil
}

//	nemo_Getrange
//	nemo_Setrange

// StrLen Return the length of a key
func (nemo *NEMO) StrLen(key []byte) (int64, error) {
	var cLen C.int64_t
	var cErr *C.char

	C.nemo_Strlen(nemo.c, goByte2char(key), C.size_t(len(key)), &cLen, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cLen), nil
}
