# syscall — System-call sequence verification via strace/dtruss

**Path:** `test/syscall/`
**Language:** Python (runner), C (test programs in subdirectories)
**Storage mode:** General
**Components under test:** file I/O durability guarantees (`fdatasync`, `fsync`, `ftruncate`, `pwrite64`), WiredTiger file open/close/create sequences, lock file management, turtle file updates, history store file creation

## Overview

The syscall test verifies that WiredTiger makes precisely the system calls required for its durability guarantees, in the correct order. The Python driver (`syscall.py`) runs each C test program under `strace` (Linux) or `dtruss` (macOS), captures the trace, and compares it against a `.run` template file. Template files use a C-preprocessor-friendly syntax with `TRACE(...)`, `RUN(...)`, `SYSTEM(...)` directives plus `...` wildcards to describe the expected system-call sequence. A mismatch (missing, extra, or reordered syscall) is a test failure.

## Test Scenarios / Cases

### Scenario: `wiredtiger_open` file creation and durability sequence (`wt2336_base/base.run`)
- **What it tests:** The full sequence of file operations during `wiredtiger_open`, `session->create` (table `hello`), `session->drop`, and `conn->close`. Specifically verifies:
  - The lock file (`WiredTiger.lock`) is opened and a sentinel written.
  - The `WiredTiger` descriptor file is written and `fdatasync`-ed (Linux only).
  - `WiredTiger.basecfg.set` is atomically written and synced.
  - The directory is `fdatasync`-ed after new file creation (Linux).
  - `WiredTiger.wt` (metadata B-tree) and `WiredTigerHS.wt` (history store) are created with correct page writes and syncs.
  - The turtle file (`WiredTiger.turtle.set`) is written atomically.
  - Creating a user table (`hello.wt`) triggers directory fdatasync and file fdatasync.
  - All of the above are repeated for the `conn->close` path (checkpoint and turtle update).
- **Components:** File manager, lock file, block manager (`pwrite64`, `ftruncate`), turtle file, metadata B-tree, history store, durability (fdatasync/fsync)
- **Notes:** Uses `O_CLOEXEC`, `O_NOATIME` flags. On macOS, `dtruss` intercepts `openat` instead of `open`; the `.run` file handles this with `#ifdef WT_USE_OPENAT` macros. The `pwrite64` syscall is normalised to `pwrite` for cross-platform matching. Variable fd values are bound to named variables (e.g., `lock`, `fd`, `wt`, `hello`) for use in subsequent assertions.

### Scenario: Cross-platform handling (Linux vs. Darwin)
- **What it tests:** The `SYSTEM("Linux")` / `SYSTEM("Darwin")` directive in the `.run` file gates whether a given run file applies. On Darwin, `dtruss` is used instead of `strace`, and `ftruncate` has an extra argument. The Python runner skips tests targeting a different platform rather than failing them.
- **Components:** Platform abstraction in syscall tracing, `openat` vs `open`
- **Notes:** `#ifdef __linux__` / `#else` blocks in the `.run` file produce different expected sequences per platform.

### Scenario: Variable binding and ASSERT expressions
- **What it tests:** The runner's expression evaluator binds the return value of an `open`/`openat` call to a named variable (e.g., `lock = OPEN(...)` binds `lock` to the returned fd). Subsequent calls that use `lock` as an argument are matched against the same bound value, verifying that WiredTiger uses the correct fd for each subsequent operation.
- **Components:** File descriptor tracking, system-call argument matching
- **Notes:** `ASSERT_EQ(close(fd), 0)` checks both the syscall arguments and the return code. `calls_returning_zero` lists syscalls (`close`, `ftruncate`, `fdatasync`) that are implicitly asserted to return 0.

## Coverage Notes

The syscall test is uniquely capable of detecting durability regressions that would be invisible to functional tests: for example, if a future change accidentally removes an `fdatasync` before a file is closed, no other test would catch it unless it also ran a crash and checked for data loss. It provides a deterministic, auditable record of WiredTiger's expected I/O behaviour. Gaps: only one test program (`wt2336_base`) is currently present; the suite does not cover checkpointing syscall sequences, WAL write sequences, or tiered/disaggregated I/O; it only runs on Linux and macOS (no Windows support); the template is sensitive to implementation changes and may require updates when internal file management changes.
