package gonemo

// #include "nemo_c.h"
// #include <stdlib.h>
import "C"

func (nemo *NEMO) RangeDel(start []byte, end []byte, limit int64) {
	C.nemo_RangeDel(nemo.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.uint64_t(limit),
	)
}

func (nemo *NEMO) RangeDelWithHandle(db *DBWithTTL, start []byte, end []byte, limit int64) {
	C.nemo_RangeDelWithHandle(nemo.c, db.c,
		goByte2char(start), C.size_t(len(start)),
		goByte2char(end), C.size_t(len(end)),
		C.uint64_t(limit),
	)
}
