---
name: wt-build
description: Use this skill when the user wants to build or compile WiredTiger, mentions "build", "compile", "ninja", "cmake", or asks to rebuild after making changes. Handles all build variants including fast builds, sanitizer builds, and force rebuilds.
version: 1.1.0
---

# WiredTiger Build & Reproduction

Use this path to:
- build or compile WiredTiger (any variant)
- reproduce a BF failure locally
- propose and verify a fix

---

## Building WiredTiger

All builds use CMake + Ninja with `-DPYTHON3_REQUIRED_VERSION=3.10`.

### Locating the WiredTiger source root

Before building, find the repo root — the directory containing `CMakeLists.txt`. Call it `$WT_SRC`.

- If currently inside a worktree (path contains `.claude/worktrees/`), `$WT_SRC` is the worktree root.
- Otherwise walk up from the current working directory until you find `CMakeLists.txt`, or ask the user.

### Build directory convention

Create build directories **relative to `$WT_SRC`**, not at a hardcoded absolute path.

| Variant | Suggested dir name |
|---|---|
| Default debug | `build/` |
| AddressSanitizer | `build_asan/` |
| MemorySanitizer | `build_msan/` |
| ThreadSanitizer | `build_tsan/` |
| UBSan | `build_ubsan/` |
| Release | `build_release/` |

### Build targets

| Ninja target | What it builds |
|---|---|
| *(none / all)* | Everything |
| `wt` | Just the `wt` CLI binary — fastest after C source changes |
| `t` | Test binaries only |

### Common build commands

**Full build (first time or after clean)**
```bash
mkdir -p $WT_SRC/build && cd $WT_SRC/build && cmake -DPYTHON3_REQUIRED_VERSION=3.10 -G Ninja .. && ninja
```

**Incremental rebuild (most common — source was edited, build dir exists)**
```bash
cd $WT_SRC/build && ninja
cd $WT_SRC/build && ninja wt   # CLI only
cd $WT_SRC/build && ninja t    # test binaries only
```

**Force clean rebuild (corrupted build state)**
```bash
rm -rf $WT_SRC/build && mkdir -p $WT_SRC/build && cd $WT_SRC/build && cmake -DPYTHON3_REQUIRED_VERSION=3.10 -G Ninja .. && ninja
```

**Sanitizer builds**
```bash
# ASan
mkdir -p $WT_SRC/build_asan && cd $WT_SRC/build_asan && cmake -DPYTHON3_REQUIRED_VERSION=3.10 -DCMAKE_BUILD_TYPE=ASan -G Ninja .. && ninja
# TSan
mkdir -p $WT_SRC/build_tsan && cd $WT_SRC/build_tsan && cmake -DPYTHON3_REQUIRED_VERSION=3.10 -DCMAKE_BUILD_TYPE=TSan -G Ninja .. && ninja
# UBSan
mkdir -p $WT_SRC/build_ubsan && cd $WT_SRC/build_ubsan && cmake -DPYTHON3_REQUIRED_VERSION=3.10 -DCMAKE_BUILD_TYPE=UBSan -G Ninja .. && ninja
# Release
mkdir -p $WT_SRC/build_release && cd $WT_SRC/build_release && cmake -DPYTHON3_REQUIRED_VERSION=3.10 -DCMAKE_BUILD_TYPE=Release -G Ninja .. && ninja
```

### Instructions

1. **Locate `$WT_SRC`** — find `CMakeLists.txt` relative to the current directory. Never hardcode an absolute path.
2. **Prefer incremental** — if the build dir exists, just `cd <build_dir> && ninja [target]`.
3. **Choose the right target**: `wt` for CLI-only changes, `t` for test-only, bare `ninja` for everything.
4. **Create the build dir if missing** — run the full `mkdir + cmake + ninja` pipeline only on first build or after an explicit clean.

---

## Reproducing a BF Failure

### Step 1: Extract the repro command from Evergreen

`evg_get_raw_task_logs` with `log_type=agent` — find the exact command CI ran:
```
python3 ../test/suite/run.py test_checkpoint ...
./t -C <config> ...
ctest -R <test-name> ...
```

Also extract:
- CMake build flags (e.g. `-DHAVE_DIAGNOSTIC=1`, `-DCMAKE_BUILD_TYPE=Debug`)
- Any env vars set before the test command

### Step 2: Match the build variant

If CI ran with ASan, reproduce with `build_asan/`. Build first if the directory doesn't exist.

### Step 3: Run the reproduction

For test/format runs and parallel repro:
→ **@skills/wiredtiger-test-format/SKILL.md** — format runs, tracing, tmux workers, stop-on-fail

For Python suite:
```bash
python3 ../test/suite/run.py <test_name> -j1
```

For ctest:
```bash
ctest --test-dir build -R <test_regex> --repeat until-fail:<N>
```

For data directory inspection after a failure: → **@wt-cli.md**

### Step 4: Capture artifacts

Always record:
- Exact command line and config
- Build variant and branch/commit
- First failure: worker dir, `stdout.log`, `stderr.log`, first error line in stderr

### Step 5: Propose a fix

State the narrowest change that addresses the root cause:
- File and approximate line range
- What changes and why it fixes the violated invariant
- Any risk of side effects

### Step 6: Verify the fix

Re-run in validation mode with the fix applied:
- For format: 4–8 workers, 20–50 iterations
- For Python suite: `python3 ../test/suite/run.py <test> -j4 --repeat 20`
- Report pass/fail rate before and after

---

## Output format

### Repro
- mode: (investigation | validation)
- command:
- config:
- build variant:
- workers:
- result: reproduced | not reproduced | inconclusive
- failure rate: X/N runs
- first failure dir / log path:

### Fix proposal
One paragraph describing the change and why it addresses the root cause.

### Verification
Pass/fail rate after fix applied.

### Next steps
1. ...
