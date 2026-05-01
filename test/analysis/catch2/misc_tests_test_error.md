# test_error — WT_TRET and WT_TRET_ERROR_OK error priority macro tests

**File:** `test/catch2/misc_tests/test_error.cpp`
**Storage mode:** General
**Components under test:** `WT_TRET`, `WT_TRET_ERROR_OK`
**Test type:** Unit

## TEST_CASE: "WT_TRET error priority" [error]
Tests that `WT_TRET` updates `ret` only when the new error has higher priority than the current value.

### SECTION: "no existing error — new error wins"
- **What it tests:** When `ret == 0`, any non-zero new error is stored.

### SECTION: "existing non-panic error — new error wins if higher priority"
- **What it tests:** A regular error code is replaced by a more severe error (e.g., `WT_PANIC`).

### SECTION: "WT_PANIC always wins"
- **What it tests:** `WT_PANIC` is never overwritten by any subsequent error.

### SECTION: "WT_RUN_RECOVERY priority"
- **What it tests:** `WT_RUN_RECOVERY` is higher priority than regular errors but lower than `WT_PANIC`.

### SECTION: "same error retained"
- **What it tests:** When `ret` already holds a given error and the same code is returned again, `ret` is unchanged.

### SECTION: "zero new error does not overwrite"
- **What it tests:** `WT_TRET(0)` is a no-op; the existing error is preserved.

### SECTION: (additional priority ordering sections)
- **What it tests:** Various combinations of POSIX errors, WT-specific errors, and zero verify the complete priority ordering.

## TEST_CASE: "WT_TRET_ERROR_OK error priority" [error]
Tests that `WT_TRET_ERROR_OK` updates `ret` only when the new error has higher priority, and additionally allows specific error codes to be treated as non-errors.

### SECTION: (matching sections from WT_TRET)
- **What it tests:** Same priority rules as `WT_TRET` but with a declared set of OK error codes.

### SECTION: "OK error code is suppressed"
- **What it tests:** When the new error is in the OK set, it is not stored in `ret` (treated as 0).

### SECTION: "WT_NOTFOUND as OK"
- **What it tests:** `WT_NOTFOUND` declared as OK is suppressed; an existing error is preserved.
