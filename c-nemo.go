package gonemo

// #cgo CXXFLAGS: -std=c++11
// #cgo CPPFLAGS: -I../c-nemo/internal/include
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #cgo LDFLAGS: -lsnappy -lbz2 -lz -ljemalloc -lm -lstdc++
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all -lrt -lpthread
import "C"

import (
	// import c-nemo library
	_ "github.com/deepfabric/c-nemo"
)
