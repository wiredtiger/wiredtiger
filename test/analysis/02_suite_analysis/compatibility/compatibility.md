# compatibility — Cross-version forward/backward compatibility testing

**Path:** `test/compatibility/`
**Storage mode:** General
**Components under test:** on-disk format, write-ahead log (WAL), checkpoint, backup, file import, upgrade/downgrade, FLCS deprecation, build system detection

## Overview

The compatibility suite validates that WiredTiger databases written by one release can be correctly read, verified, and operated on by a different release. It has two complementary layers:

1. **Shell layer** (`compatibility_test_for_releases.sh` + `meta/versions.sh`): A large Bash script that clones and builds multiple WiredTiger release branches, runs the `test/format` and `test/checkpoint` programs to generate on-disk data, and then cross-verifies or upgrade/downgrades that data using binaries from adjacent releases. Version lists are centralised in `meta/versions.sh` and consumed by both the shell script and the Python framework.

2. **Python layer** (`common/` + `suite/`): A unittest-based framework (`suite/compatibility_test.py`) built on top of the regular WiredTiger Python test infrastructure. It clones/builds branches as needed, then runs individual test functions in-process under the correct version's shared library by spawning a fresh Python subprocess with the appropriate `sys.path` pointing at the target branch's build. Individual test files live in `suite/` and inherit from `CompatibilityTestCase`.

Branch metadata (`WTVersion`, `WTBranches`) is provided by `common/compatibility_version.py` and `common/compatibility_config.py`, which parse `meta/versions.sh` at import time so the Python suite and shell script always use the same release list.

---

## Test Scenarios

### Scenario: Newer-release backward-compatibility (`-n` flag)
- **What it tests:** Builds a set of newer MongoDB release branches (`develop` through `mongodb-4.4`), runs `test/format` (row access method) and `test/checkpoint` on each, then uses each branch's binary to verify the data files produced by the next-older branch. Also runs `wt dump`/`wt load` to exercise the dump-and-reload path. Finally runs `upgrade_downgrade`, which alternates each branch's format binary on the other branch's data directory.
- **Components:** `test/format`, `test/checkpoint`, `wt` CLI dump/load, upgrade/downgrade logic, snappy/reverse-collator/rotn extension loading
- **Notes:** Row access method only for newer branches. Build-system path fixup (`fixup_format_extension_paths`) handles autoconf-vs-cmake library path differences when crossing the MongoDB 6.0/5.0 boundary.

### Scenario: Older-release compatibility (`-o` flag)
- **What it tests:** Builds `mongodb-4.4` and `mongodb-4.2`, runs `test/format` with both row and variable-length column-store (`var`) access methods, then verifies each release's data with the adjacent release's binary.
- **Components:** `test/format`, `wt` CLI verify, row and var access methods
- **Notes:** The only mode that exercises variable-length column store. Uses a single shared `CONFIG_default` instead of per-branch config files.

### Scenario: WiredTiger standalone releases (`-w` flag)
- **What it tests:** Builds `develop` plus the two most recent standalone WiredTiger releases (determined via numeric `git tag` sorting), generates data with `test/format` and `test/checkpoint`, then verifies across the three.
- **Components:** `test/format`, `test/checkpoint`, `wt` CLI verify
- **Notes:** Standalone WiredTiger releases use numeric version tags (e.g. `10.0.0`). Branches older than 11.0.0 used autoconf; newer branches use CMake.

### Scenario: Patch-version upgrade/downgrade (`-p` flag)
- **What it tests:** For each configured release branch (8.3 down to 4.4), builds the tip of the branch and also a randomly-chosen tagged patch release of that branch, runs `test/checkpoint` on both, then cross-verifies. Applies a patch for WT-8708 if it is missing from the picked patch version.
- **Components:** `test/checkpoint`, version-gating logic (`is_test_checkpoint_recovery_supported`)
- **Notes:** Skips versions earlier than 4.4.9 / 5.0.3 because WT-7958 (checkpoint recovery) was not yet present. Random patch selection means the exact scenario exercised varies between runs.

### Scenario: Upgrade to latest (`-u` flag)
- **What it tests:** Fetches pre-existing databases from the `wiredtiger/mongo-tests` GitHub repo (WT-8395 test data, covering mongodb-4.4.x unclean-shutdown scenarios), then runs `test/checkpoint` from each target branch against those databases. Deliberately expects failure for databases from 4.4.[0-6] that were produced by an unclean shutdown.
- **Components:** `test/checkpoint`, external test data repository
- **Notes:** The expected-failure path for 4.4.[0-6] unclean shutdown databases is the only place in the suite that asserts a non-zero exit is correct.

### Scenario: Dirty restart (`-d` flag)
- **What it tests:** Runs `test/format` with `format.abort=1` to simulate a crash (SIGABRT/segfault), then recovers using a different release's format binary. All combinations of the `UPGRADE_TO_LATEST` branch list are exercised, subject to a compatibility gate that prevents pairing 6.0+ source with a 5.0 or earlier recovery binary (due to a fast-truncate flag introduced in 6.0).
- **Components:** `test/format` (abort mode), cross-version crash recovery
- **Notes:** `compatibility=(release=10.0.0)` is forced on the source run so the on-disk format version does not prevent the older binary from reading the files.

### Scenario: Import compatibility (`-i` flag)
- **What it tests:** Creates a small key/value `file:` object in an older release's `wt` CLI, physically copies it into a newer release's home directory, opens it with `import=(enabled,repair=true)`, then verifies it. Then rounds back with the older binary doing a dump against the newer-branch database to simulate a downgrade.
- **Components:** `wt` CLI create/write/dump/load, file import API
- **Notes:** Operates on `file:` objects directly rather than going through `table:`. Removal of `WiredTiger.basecfg` before the downgrade dump emulates MongoDB behaviour.

### Scenario: Two arbitrary versions (`-v v1 v2`)
- **What it tests:** Accepts two explicit version names, builds them, runs `test/checkpoint` on each, and cross-verifies. Both versions must support WT-7958 checkpoint recovery.
- **Components:** `test/checkpoint`
- **Notes:** Useful for one-off investigation of a specific version pair without modifying the version lists.

### Scenario: Python suite — FLCS deprecation (`suite/test_flcs_deprecate.py`)
- **What it tests:** Verifies the lifecycle of Fixed-Length Column Store (FLCS) tables across the deprecation boundary introduced in mongodb-8.3. On older branches (pre-8.3), creates and populates an FLCS table; on mongodb-8.3+, attempts to open that database and expects `WT_PANIC`. Also verifies that attempting to create a new FLCS table on 8.3+ returns `ENOTSUP`.
- **Components:** Python WiredTiger API, FLCS table format, upgrade path, error handling
- **Notes:** Requires a standalone build (`build_config = {'standalone': 'true'}`). The test is a no-op for older-branch pairs that are both pre-deprecation; it only fires on the specific pre/post boundary crossing.

### Scenario: Python suite — Checkpoint downgrade (WT-10533) (`suite/test_wt10533.py`)
- **What it tests:** Reproduces WT-10533: creates 100 rapid checkpoints on a newer branch (causing the checkpoint monotonic clock to race ahead of wall time), then opens the resulting database on an older branch and performs a backup while adding more checkpoints. Verifies that the backup can be re-opened and that the data visible in the older branch's checkpoint is consistent.
- **Components:** Python WiredTiger API, checkpoint subsystem, backup cursor, downgrade path
- **Notes:** Sets `compatibility=(release="3.3")` explicitly to allow the older branch to open the file. Uses `statistics_log` to ensure statistics are being exercised through the downgrade path.

---

## Coverage Notes

**Uniquely covered:**
- The only place in the WiredTiger test tree that exercises cross-release binary interoperability for both `test/format` and `test/checkpoint` simultaneously.
- Crash-recovery compatibility (dirty restart) across multiple version pairs is not covered anywhere else.
- FLCS deprecation upgrade/downgrade boundary (python suite) is a targeted regression guard for a one-time feature removal.
- The WT-10533 checkpoint-time monotonic drift bug is covered only in this suite.
- File import (`import=(enabled,repair=true)`) across release boundaries is tested only here.

**Gaps and limitations:**
- Column store (fixed-length, FLCS) is tested only in the Python suite for the deprecation scenario; the shell-based newer-branch flow uses row only. Variable-length column store is exercised only in the older-release (`-o`) path.
- The Python suite currently supports only branches mongodb-6.0 and newer (CMake requirement); older autoconf branches cannot be built by `compatibility_common.prepare_branch`.
- Patch-version selection is random (`$RANDOM`), so test coverage of specific patch releases is non-deterministic.
- The upgrade-to-latest scenario depends on an external GitHub repository (`wiredtiger/mongo-tests`), creating a network and repository-availability dependency.
- No TimeSeries, columnar, or encryption configurations are exercised in any cross-version scenario.
- Compression is locked to snappy (the only compressor built); lz4, zlib, and zstd cross-version paths are not tested.
