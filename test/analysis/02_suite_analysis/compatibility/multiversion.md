# multiversion — Workgen-based multiversion read/write compatibility testing

**Path:** `test/multiversion/`
**Storage mode:** General
**Components under test:** workgen runner, multiversion concurrency control (MVCC), read/write compatibility across MongoDB 4.2 and 4.4

## Overview

`wt_multiversion.sh` is a lightweight shell script that validates WiredTiger's ability to handle concurrent read and write workloads across two release versions (mongodb-4.2 "last stable" and mongodb-4.4 "latest") using the `bench/workgen/runner/multiversion.py` workload script. It clones and builds the mongodb-4.2 branch if it is not already present, then runs the workgen multiversion workload in a sequence of steps that interleave the two releases against a shared on-disk database.

The workgen `multiversion.py` runner (located in `bench/workgen/runner/`) is the actual test driver; the shell script is exclusively an orchestration wrapper.

---

## Test Scenarios

### Scenario: Initial populate on 4.4 (latest)
- **What it tests:** Runs `multiversion.py --release 4.4` on a fresh database using the latest (4.4) workgen binary. Creates and populates the on-disk structures that subsequent steps will reuse.
- **Components:** workgen runner, table creation, initial data population, MVCC
- **Notes:** This is the only step that runs without `--keep`, so it starts from a clean state.

### Scenario: Continued workload on 4.4 with keep
- **What it tests:** Runs `multiversion.py --keep --release 4.4` against the database from the previous step, continuing operations on the same files without reinitialising.
- **Components:** workgen runner, MVCC, sustained read/write workload
- **Notes:** The `--keep` flag preserves the existing database so data written in the prior step is visible.

### Scenario: Workload on 4.2 (last stable) against 4.4 database
- **What it tests:** Runs the 4.2-branch copy of `multiversion.py --keep --release 4.2` against the database that was built by the 4.4 binary. Validates that the older release can open and operate on data produced by the newer release.
- **Components:** backward compatibility, workgen runner (4.2 binary), MVCC across versions
- **Notes:** The workgen script is copied from the latest tree into the 4.2 tree before this step, ensuring the same workload definition is used on both versions. This is the primary compatibility assertion in the script.

### Scenario: Return to 4.4 after 4.2 operations
- **What it tests:** Re-runs `multiversion.py --keep --release 4.4` on the database that was last touched by the 4.2 binary, verifying that the newer release can pick up where the older left off.
- **Components:** forward compatibility, upgrade after downgrade, MVCC
- **Notes:** Completes a full round-trip: populate with 4.4 → operate with 4.2 → resume with 4.4.

---

## Coverage Notes

**Uniquely covered:**
- The only test in the WiredTiger tree that exercises the workgen `multiversion.py` runner directly as a compatibility vehicle.
- Validates a concrete round-trip: write with newer, read/write with older, then resume with newer — a pattern that mirrors real MongoDB rolling-upgrade behaviour.
- Because workgen generates a realistic mixed read/write workload, this scenario exercises MVCC conflict resolution and version-chain management across releases, which pure format-verification tests do not reach.

**Gaps and limitations:**
- The script is hard-coded to the mongodb-4.2 / mongodb-4.4 version pair. No mechanism exists to vary or extend this to newer releases without manual script modification.
- The script has not been updated since mongodb-4.4 was current; the branches it tests are now end-of-life, limiting its relevance to the current codebase.
- There is no assertion beyond process exit code; if workgen completes without crashing the test passes, regardless of whether data was correctly read, modified, or preserved.
- The workgen script is copied from the latest tree into the 4.2 tree by simple `cp`. If the workload format becomes incompatible between the two versions, this step will silently use a mismatched workload rather than failing explicitly.
- No cleanup step is defined; leftover `wiredtiger_4.2/` directories from previous runs are reused, which can mask regressions if the prior state was already broken.
- Compression, encryption, and column store configurations are not exercised.
- Only the workgen multiversion runner is tested; `test/format` and `test/checkpoint` cross-version scenarios for this version pair are covered by the separate `compatibility_test_for_releases.sh` suite.
