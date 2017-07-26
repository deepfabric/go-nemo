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
)

// Options nemo instance option
type Options struct {
	c *C.nemo_options_t
}

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
	}
}

// JSONOpt nemo options with json format
type JSONOpt struct {
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

// NewOptions Create new options for nemo
func NewOptions(nemoConf string) (*Options, *JSONOpt) {

	bytes, err := ioutil.ReadFile(nemoConf)
	if err != nil {
		fmt.Printf("Read nemo config File[%s] err: %s\n", nemoConf, err.Error())
		return nil, nil
	}

	var jopt JSONOpt
	err = json.Unmarshal(bytes, &jopt)
	if err != nil {
		fmt.Printf("Parse jsonfile[%s] err: %s\n", nemoConf, err.Error())
		return nil, nil
	}

	cOpts := C.nemo_CreateOption()

	var goOpts = C.GoNemoOpts{
		create_if_missing: C.bool(jopt.CreateIfMissing),
		write_buffer_size: C.int(jopt.WriteBufferSize * 1024 * 1024),
		max_open_files:    C.int(jopt.MaxOpenFiles),
		use_bloomfilter:   C.bool(jopt.UseBloomfilter),
		write_threads:     C.int(jopt.WriteThreads),

		// default target_file_size_base and multiplier is the same as rocksdb
		target_file_size_base:          C.int(jopt.TargetFileSizeBase * 1024 * 1024),
		target_file_size_multiplier:    C.int(jopt.TargetFileSizeMultiplier),
		compression:                    C.bool(jopt.Compression),
		max_background_flushes:         C.int(jopt.MaxBackgroundFlushes),
		max_background_compactions:     C.int(jopt.MaxBackgroundCompactions),
		max_bytes_for_level_multiplier: C.int(jopt.MaxBytesForLevelMultiplier),
	}

	C.nemo_SetOptions(cOpts, &goOpts)

	return &Options{c: cOpts}, &jopt
}

// OpenNemo return a nemo handle
func OpenNemo(opts *Options, path string) *NEMO {
	var (
		cPath = C.CString(path)
	)
	defer C.free(unsafe.Pointer(cPath))
	nemo := C.nemo_Create(cPath, opts.c)
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
