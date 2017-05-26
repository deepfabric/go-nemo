package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// Del Delete all the single key in different data structure
func (nemo *NEMO) Del(key []byte) (int64, error) {
	var (
		cErr  *C.char
		count C.int64_t
	)
	C.nemo_Del(nemo.c, goByte2char(key), C.size_t(len(key)), &count, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(count), nil
}

// MDel Delete multi keys in different data structure
func (nemo *NEMO) MDel(keys [][]byte) (int64, error) {
	var (
		cErr  *C.char
		count C.int64_t
	)
	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_MDel(nemo.c,
		C.int(len(keys)),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&count, &cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(count), nil
}

// Expire Set expire time to a single key in different data structure
func (nemo *NEMO) Expire(key []byte, second int32) (int64, error) {
	var (
		cErr *C.char
		res  C.int64_t
	)
	C.nemo_Expire(nemo.c, goByte2char(key), C.size_t(len(key)), C.int32_t(second), &res, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(res), nil
}

// TTL Return ttl time of a single key
func (nemo *NEMO) TTL(key []byte) (int64, error) {
	var (
		cErr  *C.char
		count C.int64_t
	)
	C.nemo_TTL(nemo.c, goByte2char(key), C.size_t(len(key)), &count, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(count), nil
}

// Persist Remove the expiration from a key
func (nemo *NEMO) Persist(key []byte) (int64, error) {
	var (
		cErr  *C.char
		count C.int64_t
	)
	C.nemo_Persist(nemo.c, goByte2char(key), C.size_t(len(key)), &count, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(count), nil
}

// Expireat Set the expiration for a key as a UNIX timestamp
func (nemo *NEMO) Expireat(key []byte, timestamp int32) (int64, error) {
	var (
		cErr *C.char
		res  C.int64_t
	)
	C.nemo_Expireat(nemo.c, goByte2char(key), C.size_t(len(key)), C.int32_t(timestamp), &res, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(res), nil
}

// Type Return the key's type
func (nemo *NEMO) Type(key []byte) (*string, error) {
	var (
		cErr    *C.char
		keyType *C.char
	)
	C.nemo_Type(nemo.c, goByte2char(key), C.size_t(len(key)), &keyType, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}
	res := C.GoString(keyType)
	C.free(unsafe.Pointer(keyType))
	return &res, nil
}

// Exists Return true if the key does exist
func (nemo *NEMO) Exists(keys [][]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}
	C.nemo_Exists(nemo.c, C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&cRes,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cRes), nil
}

// func KMDel

// func KScan
