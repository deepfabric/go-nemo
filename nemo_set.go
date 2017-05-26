package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// SAdd Add muli member into a set
func (nemo *NEMO) SAdd(key []byte, members ...[]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(members)

	cmemberlist := make([]*C.char, l)
	cmemberlen := make([]C.size_t, l)

	for i, member := range members {
		cmemberlist[i] = goBytedup2char(member)
		cmemberlen[i] = C.size_t(len(member))
	}

	C.nemo_SMAdd(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&cmemberlist[0])),
		(*C.size_t)(unsafe.Pointer(&cmemberlen[0])),
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRes), nil
}

// SRem Remove multi member from a set
func (nemo *NEMO) SRem(key []byte, members ...[]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(members)

	cmemberlist := make([]*C.char, l)
	cmemberlen := make([]C.size_t, l)

	for i, member := range members {
		cmemberlist[i] = goBytedup2char(member)
		cmemberlen[i] = C.size_t(len(member))
	}

	C.nemo_SMRem(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&cmemberlist[0])),
		(*C.size_t)(unsafe.Pointer(&cmemberlen[0])),
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRes), nil
}

// SCard Return the sum of the member in a set
func (nemo *NEMO) SCard(key []byte) (int64, error) {
	var cSize C.int64_t
	var cErr *C.char
	C.nemo_SCard(nemo.c, goByte2char(key), C.size_t(len(key)), &cSize, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cSize), nil
}

// SMembers Return all the members in a set
func (nemo *NEMO) SMembers(key []byte) ([][]byte, error) {
	var n C.int
	var memberlist **C.char
	var memberlistlen *C.size_t
	var cErr *C.char
	C.nemo_SMembers(nemo.c, goByte2char(key), C.size_t(len(key)), &memberlist, &memberlistlen, &n, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, res
	}

	if n == 0 {
		return nil, nil
	}
	return cstr2GoMultiByte(int(n), memberlist, memberlistlen), nil

}

// SUnionStore Do the union operation on multi set and store the result into a dest set
func (nemo *NEMO) SUnionStore(dest []byte, keys [][]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SUnionStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&cRes,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRes), nil
}

// SUnion Do the union operation on multi set and return the result
func (nemo *NEMO) SUnion(keys [][]byte) ([][]byte, error) {
	var n C.int
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char

	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SUnion(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&n,
		&vallist, &vallistlen,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

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

// SInterStore Do the intersect operation on multi set and store the result into a dest set
func (nemo *NEMO) SInterStore(dest []byte, keys [][]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SInterStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&cRes,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRes), nil
}

// SInter Do the intersect operation on multi set and return the result
func (nemo *NEMO) SInter(keys [][]byte) ([][]byte, error) {
	var n C.int
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char

	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SInter(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&n,
		&vallist, &vallistlen,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

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

// SDiffStore Do the diff operation on multi set and store the result into a dest set
func (nemo *NEMO) SDiffStore(dest []byte, keys [][]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SDiffStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&cRes,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRes), nil
}

// SDiff Do the diff operation on multi set and return the result
func (nemo *NEMO) SDiff(keys [][]byte) ([][]byte, error) {
	var n C.int
	var vallist **C.char
	var vallistlen *C.size_t
	var cErr *C.char

	l := len(keys)
	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
	}

	C.nemo_SDiff(nemo.c,
		C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		&n,
		&vallist, &vallistlen,
		&cErr,
	)

	for _, key := range ckeylist {
		C.free(unsafe.Pointer(key))
	}

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

// SIsMember Return true if the member does in the set
func (nemo *NEMO) SIsMember(key []byte, member []byte) (bool, error) {
	var cIfExist C.bool
	var cErr *C.char
	C.nemo_SIsMember(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(member), C.size_t(len(member)),
		&cIfExist, &cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return false, res
	}
	return bool(cIfExist), nil
}

// SPop Remove and return a random member of Set
// If set is null, return false
func (nemo *NEMO) SPop(key []byte) (exist bool, value []byte, err error) {
	var cMember *C.char
	var cLen C.size_t
	var cErr *C.char
	var cRes C.int64_t

	C.nemo_SPop(nemo.c, goByte2char(key), C.size_t(len(key)), &cMember, &cLen, &cRes, &cErr)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return false, nil, err
	}

	if cRes == 0 {
		return false, nil, nil
	}

	value = C.GoBytes(unsafe.Pointer(cMember), C.int(cLen))
	C.free(unsafe.Pointer(cMember))
	return true, value, nil
}

// SRandomMember Get one or multiple random members from a set
func (nemo *NEMO) SRandomMember(key []byte, count int) ([][]byte, error) {
	var n C.int
	var memberlist **C.char
	var memberlistlen *C.size_t
	var cRes C.int64_t
	var cErr *C.char
	C.nemo_SRandomMember(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		&n, &memberlist, &memberlistlen,
		C.int(count),
		&cRes,
		&cErr,
	)

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, err
	}

	if n == 0 || cRes == 0 {
		return nil, nil
	}

	return cstr2GoMultiByte(int(n), memberlist, memberlistlen), nil
}

// SMove Move a member from one set to another
func (nemo *NEMO) SMove(srckey []byte, destkey []byte, member []byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t

	C.nemo_SMove(nemo.c,
		goByte2char(srckey), C.size_t(len(srckey)),
		goByte2char(destkey), C.size_t(len(destkey)),
		goByte2char(member), C.size_t(len(member)),
		&cRes,
		&cErr,
	)

	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}

	return int64(cRes), nil

}
