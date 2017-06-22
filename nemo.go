package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
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

// NewOptions Create new options for nemo
func NewOptions() *Options {
	cOpts := C.nemo_CreateOption()

	var goOpts = C.GoNemoOpts{
		create_if_missing: C.bool(true),
		write_buffer_size: C.int(64 * 1024 * 1024),
		max_open_files:    C.int(5000),
		use_bloomfilter:   C.bool(true),
		write_threads:     C.int(71),

		// default target_file_size_base and multiplier is the same as rocksdb
		target_file_size_base:          C.int(20 * 1024 * 1024),
		target_file_size_multiplier:    C.int(1),
		compression:                    C.bool(true),
		max_background_flushes:         C.int(1),
		max_background_compactions:     C.int(1),
		max_bytes_for_level_multiplier: C.int(10),
	}

	C.nemo_SetOptions(cOpts, &goOpts)

	return &Options{c: cOpts}
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
