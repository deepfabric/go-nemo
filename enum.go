package gonemo

// DBType is the type of nemo-rocksdb
type DBType int

const (
	// NoneDB DBType
	NoneDB DBType = iota
	// KvDB DBType
	KvDB
	// HashDB DBType
	HashDB
	// ListDB DBType
	ListDB
	// ZSetDB DBType
	ZSetDB
	// SetDB DBType
	SetDB
	// ALL DBType
	ALL
)

// BitOpType is the bitmap operation type
type BitOpType int

const (
	// BitOpNot BitOpTye
	BitOpNot BitOpType = iota
	// BitOpAnd BitOpTye
	BitOpAnd
	// BitOpOr BitOpTye
	BitOpOr
	// BitOpXor BitOpTye
	BitOpXor
	// BitOpDefault BitOpTye
	BitOpDefault
)

// Aggregate is the type of aggregate
type Aggregate int

const (
	// SUM Aggregate
	SUM Aggregate = iota
	// MIN Aggregate
	MIN
	// MAX Aggregate
	MAX
)
