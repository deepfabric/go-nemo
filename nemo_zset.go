package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"unsafe"
)

// ZAdd Add new member with score into zset.
func (nemo *NEMO) ZAdd(key []byte, score float64, member []byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	C.nemo_ZAdd(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.double(score),
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

// ZCard Return the element count of the zset.
func (nemo *NEMO) ZCard(key []byte) (int64, error) {
	var cSize C.int64_t
	var cErr *C.char
	C.nemo_ZCard(nemo.c, goByte2char(key), C.size_t(len(key)), &cSize, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cSize), nil
}

// ZCount Return the count between score range of the zset.
func (nemo *NEMO) ZCount(key []byte, begin float64, end float64, IsLo bool, IsRo bool) (int64, error) {
	var cSize C.int64_t
	var cErr *C.char
	C.nemo_ZCount(nemo.c, goByte2char(key), C.size_t(len(key)),
		C.double(begin), C.double(end),
		&cSize,
		C.bool(IsLo), C.bool(IsRo),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, res
	}
	return int64(cSize), nil
}

// ZIncrby Increment the score int the zset by a float value
func (nemo *NEMO) ZIncrby(key []byte, member []byte, by float64) ([]byte, error) {
	var cRes *C.char
	var cLen C.size_t
	var cErr *C.char

	C.nemo_ZIncrby(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(member), C.size_t(len(member)),
		C.double(by), &cRes, &cLen, &cErr,
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

// ZRange Return zset memeber between start and stop
func (nemo *NEMO) ZRange(key []byte, start int64, stop int64) ([]float64, [][]byte, error) {
	var n C.size_t
	var cScoreList *C.double
	var memberlist **C.char
	var memberlistlen *C.size_t
	var cErr *C.char

	C.nemo_ZRange(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.int64_t(start), C.int64_t(stop),
		&n,
		&cScoreList, &memberlist, &memberlistlen,
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, nil, res
	}

	if n == 0 {
		return nil, nil, nil
	}
	cScoreListSlice := cDoubles2Slice(cScoreList, int(n))
	ScoreList := make([]float64, int(n))
	for i := range ScoreList {
		ScoreList[i] = float64(cScoreListSlice[i])
	}
	C.free(unsafe.Pointer(cScoreList))
	return ScoreList, cstr2GoMultiByte(int(n), memberlist, memberlistlen), nil

}

// ZUnionStore Add multiple sorted sets and store the resulting sorted set in a new key
func (nemo *NEMO) ZUnionStore(dest []byte, keys [][]byte, weights []float64, aggtype Aggregate) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	cWeights := make([]C.double, l)

	if len(keys) != len(weights) {
		return 0, errors.New("keys len != weights len")
	}

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
		cWeights[i] = C.double(weights[i])
	}

	C.nemo_ZUnionStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
		C.int(l), C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		(*C.double)(unsafe.Pointer(&cWeights[0])),
		C.int(aggtype),
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

// ZInterStore Intersect multiple sorted sets and store the resulting sorted set in a new key
func (nemo *NEMO) ZInterStore(dest []byte, keys [][]byte, weights []float64, aggtype Aggregate) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(keys)

	ckeylist := make([]*C.char, l)
	ckeylen := make([]C.size_t, l)
	cWeights := make([]C.double, l)

	if len(keys) != len(weights) {
		return 0, errors.New("keys len != weights len")
	}

	for i, key := range keys {
		ckeylist[i] = goBytedup2char(key)
		ckeylen[i] = C.size_t(len(key))
		cWeights[i] = C.double(weights[i])
	}

	C.nemo_ZInterStore(nemo.c,
		goByte2char(dest), C.size_t(len(dest)),
		C.int(l), C.int(l),
		(**C.char)(unsafe.Pointer(&ckeylist[0])),
		(*C.size_t)(unsafe.Pointer(&ckeylen[0])),
		(*C.double)(unsafe.Pointer(&cWeights[0])),
		C.int(aggtype),
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

// ZRangebyScore Return a range of members in a sorted set, by score
func (nemo *NEMO) ZRangebyScore(key []byte, mn float64, mx float64, IsLo bool, IsRo bool) ([]float64, [][]byte, error) {
	var n C.int
	var cScoreList *C.double
	var memberlist **C.char
	var memberlistlen *C.size_t
	var cErr *C.char

	C.nemo_ZRangebyScore(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		C.double(mn), C.double(mx),
		&n,
		&cScoreList, &memberlist, &memberlistlen,
		C.bool(IsLo), C.bool(IsRo),
		&cErr,
	)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return nil, nil, res
	}

	if n == 0 {
		return nil, nil, nil
	}
	cScoreListSlice := cDoubles2Slice(cScoreList, int(n))
	ScoreList := make([]float64, int(n))
	for i := range ScoreList {
		ScoreList[i] = float64(cScoreListSlice[i])
	}
	C.free(unsafe.Pointer(cScoreList))
	return ScoreList, cstr2GoMultiByte(int(n), memberlist, memberlistlen), nil

}

// ZRem Remove one or more members from a sorted set
func (nemo *NEMO) ZRem(key []byte, members ...[]byte) (int64, error) {
	var cErr *C.char
	var cRes C.int64_t
	l := len(members)

	cmemberlist := make([]*C.char, l)
	cmemberlen := make([]C.size_t, l)

	for i, member := range members {
		cmemberlist[i] = goBytedup2char(member)
		cmemberlen[i] = C.size_t(len(member))
	}

	C.nemo_ZMRem(nemo.c,
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

// ZRank Determine the index of a member in a sorted set
func (nemo *NEMO) ZRank(key []byte, member []byte) (int64, error) {
	var cErr *C.char
	var cRank C.int64_t
	C.nemo_ZRank(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(member), C.size_t(len(member)),
		&cRank,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRank), nil
}

// ZRevRank Determine the index of a member in a sorted set, with scores ordered from high to low
func (nemo *NEMO) ZRevRank(key []byte, member []byte) (int64, error) {
	var cErr *C.char
	var cRank C.int64_t
	C.nemo_ZRevrank(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(member), C.size_t(len(member)),
		&cRank,
		&cErr,
	)
	if cErr != nil {
		err := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return 0, err
	}
	return int64(cRank), nil
}

// ZScore Return the score of a member
func (nemo *NEMO) ZScore(key []byte, member []byte) (exist bool, score float64, err error) {
	var cErr *C.char
	var cScore C.double
	var cRes C.int64_t

	C.nemo_ZScore(nemo.c,
		goByte2char(key), C.size_t(len(key)),
		goByte2char(member), C.size_t(len(member)),
		&cScore,
		&cRes,
		&cErr,
	)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return false, 0, err
	}

	if cRes == 0 {
		return false, 0, nil
	}

	return true, float64(cScore), nil
}

//nemo_ZRangebylex

//nemo_ZLexcount

//nemo_ZRemrangebylex

//nemo_ZRemrangebyrank

//nemo_ZRemrangebyscore
