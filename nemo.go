package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"unsafe"

	slab "github.com/funny/slab"
)

// Options go-nemo instance option
type Options struct {
	c *C.nemo_options_t
	p *MemPoolOpt
}

// MemPool for go-nemo
var MemPool *slab.SyncPool

// NEMO instance handle
type NEMO struct {
	c      *C.nemo_t
	dbPath string
	opts   *Options
}

// NewDefaultOptions create default option
func NewDefaultOptions() *Options {
	opts := C.nemo_CreateOption()

	return &Options{
		c: opts,
		p: &MemPoolOpt{
			MinMemPoolChunkSize: 8,        // The smallest chunk size is 64B.
			MaxMemPoolChunkSize: 4 * 1024, // The largest chunk size is 64KB.
			MemPoolFactor:       2,        // Power of 2 growth in chunk size.
		},
	}
}

// RocksOpt rocksdb options for go nemo
type RocksOpt struct {
	// options for nemo rocksdb
	CreateIfMissing            bool `json:"create_if_missing"`
	WriteBufferSize            int  `json:"write_buffer_size"`
	MaxOpenFiles               int  `json:"max_open_files"`
	UseBloomfilter             bool `json:"use_bloomfilter"`
	WriteThreads               int  `json:"write_threads"`
	TargetFileSizeBase         int  `json:"target_file_size_base"`
	TargetFileSizeMultiplier   int  `json:"target_file_size_multiplier"`
	Compression                bool `json:"compression"`
	MaxBackgroundFlushes       int  `json:"max_background_flushes"`
	MaxBackgroundCompactions   int  `json:"max_background_compactions"`
	MaxBytesForLevelMultiplier int  `json:"max_bytes_for_level_multiplier"`
}

// MemPoolOpt memory pool option for go-nemo
type MemPoolOpt struct {
	// options for golang mem pool
	MinMemPoolChunkSize int `json:"min_mempool_chunk_size"`
	MaxMemPoolChunkSize int `json:"max_mempool_chunk_size"`
	MemPoolFactor       int `json:"mempool_factor"`
}

// JSONOpt nemo options with json format
type JSONOpt struct {
	// options for nemo rocksdb
	Db RocksOpt `json:"rocksdb"`
	// options for golang mem pool
	Pool MemPoolOpt `json:"mem_pool"`
}

// NewOptions Create new options for nemo
func NewOptions(nemoConf string) (*Options, *JSONOpt) {

	bytes, err := ioutil.ReadFile(nemoConf)
	if err != nil {
		fmt.Printf("Read nemo config File[%s] err: %s\n", nemoConf, err.Error())
		panic("init error!")
	}

	var jopt JSONOpt
	err = json.Unmarshal(bytes, &jopt)
	if err != nil {
		fmt.Printf("Parse jsonfile[%s] err: %s\n", nemoConf, err.Error())
		panic("init error!")
	}

	cOpts := C.nemo_CreateOption()

	var goOpts = C.GoNemoOpts{
		create_if_missing: C.bool(jopt.Db.CreateIfMissing),
		write_buffer_size: C.int(jopt.Db.WriteBufferSize * 1024 * 1024),
		max_open_files:    C.int(jopt.Db.MaxOpenFiles),
		use_bloomfilter:   C.bool(jopt.Db.UseBloomfilter),
		write_threads:     C.int(jopt.Db.WriteThreads),

		// default target_file_size_base and multiplier is the same as rocksdb
		target_file_size_base:          C.int(jopt.Db.TargetFileSizeBase * 1024 * 1024),
		target_file_size_multiplier:    C.int(jopt.Db.TargetFileSizeMultiplier),
		compression:                    C.bool(jopt.Db.Compression),
		max_background_flushes:         C.int(jopt.Db.MaxBackgroundFlushes),
		max_background_compactions:     C.int(jopt.Db.MaxBackgroundCompactions),
		max_bytes_for_level_multiplier: C.int(jopt.Db.MaxBytesForLevelMultiplier),
	}

	C.nemo_SetOptions(cOpts, &goOpts)

	return &Options{c: cOpts, p: &jopt.Pool}, &jopt
}

// OpenNemo return a nemo handle
func OpenNemo(opts *Options, path string) *NEMO {
	var (
		cPath = C.CString(path)
	)
	defer C.free(unsafe.Pointer(cPath))
	nemo := C.nemo_Create(cPath, opts.c)
	MemPool = slab.NewSyncPool(
		opts.p.MinMemPoolChunkSize,
		opts.p.MaxMemPoolChunkSize,
		opts.p.MemPoolFactor,
	)
	return &NEMO{
		c:      nemo,
		dbPath: path,
		opts:   opts,
	}
}

// Close nemo instance
func (nemo *NEMO) Close() {
	C.nemo_free(nemo.c)
}

// Compact do all compact
func (nemo *NEMO) Compact(dbType DBType, sync bool) error {
	var cErr *C.char
	C.nemo_Compact(nemo.c, C.int(dbType), C.bool(sync), &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// RunBGTask call background task
func (nemo *NEMO) RunBGTask() error {
	var cErr *C.char
	C.nemo_RunBGTask(nemo.c, &cErr)
	if cErr != nil {
		res := errors.New(C.GoString(cErr))
		C.free(unsafe.Pointer(cErr))
		return res
	}
	return nil
}

// GetCurrentTaskType return current task type
func (nemo *NEMO) GetCurrentTaskType() *string {
	cTaskType := C.nemo_GetCurrentTaskType(nemo.c)
	res := C.GoString(cTaskType)
	C.free(unsafe.Pointer(cTaskType))
	return &res
}
