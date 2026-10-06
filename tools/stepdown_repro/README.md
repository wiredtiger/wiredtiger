# Stepdown checkpoint reproducer

## Handoff: what this branch establishes

Ticket: [WT-18724](https://jira.mongodb.org/browse/WT-18724). This is a **diagnostic branch, not a
product fix**. It reproduces a committed deletion disappearing from a disaggregated stable
checkpoint, causing the deleted row to reappear after stepdown, step-up, and process reopen.

The key distinction is persistence: a cursor-positioning change cannot repair a completed
checkpoint that still references the old live row. We proved that stale image in CI artifacts and
reproduced a specific failure mechanism locally. The exact transient execution of that mechanism
on the CI victim remains unproven; do not describe the CI root cause as fully established.

The current checkout is branch `wt-18724-stepdown-repro` in `~/wiredtiger`, based on local `develop`
at `ec83e71b7408d78418ce00239d370bb5d0a7eaa9`. It contains diagnostic hooks and the standalone driver,
logger, validator, build helper, and probability analyzer. The original format-instrumentation
commit remains on `wt-18724-window-write-trace` (`5dc6160e1c80a431ba7c622085f9af919872402d`).
No PR #14774 changes or temporary 50% failpoint probability remain applied.

### Reproduced mechanism and source map

```text
one-row stored leaf, containing the bulk value
  → pre-cutoff delete empties the reconciliation image
  → empty-image path marks tombstone WT_UPDATE_DURABLE too early
  → pre-wrapup EBUSY leaves that mark and the old stored address
  → post-cutoff mirrored neighbor must be restored rather than checkpointed
  → false durable delete suppresses "new selected update" tracking
  → empty-image restore reuses the old address and inherits checkpoint generation
  → checkpoint snapshot-skip accepts that address/generation
  → follower loads the old row; step-up has no ingest copy of the earlier delete
```

Start reading these functions (line numbers change as instrumentation evolves):

| Source | Role |
|---|---|
| `src/reconcile/rec_write.c`: `__rec_set_updates_durable` | Contract: durability marking must occur only when reconciliation cannot fail |
| Same file: `__rec_split_write`, empty-page branch | Premature durability mark before pre-wrapup failure |
| Same file: `__reconcile`, `__rec_write_err` | Random/targeted EBUSY and cleanup that leaves the mark |
| `src/reconcile/rec_visibility.c`, `reconcile_inline.h` | Update selection/save and durable-key change suppression |
| `rec_write.c`: `__rec_copy_prev_addr` | Reuse of the prior full-image address |
| `src/evict/evict_page.c`, `src/btree/bt_split.c` | Real eviction snapshot and update restoration/stamp inheritance |
| `src/btree/bt_sync.c`: `__sync_checkpoint_can_skip` | Snapshot generation permits skipping the retained image |
| `src/conn/conn_layered.c`, `conn_layered_ingest.c` | Demotion invalidates live stable state; promotion drains ingest |

### CI evidence: patch #3543

[Failing Evergreen task, execution 0](https://spruce.corp.mongodb.com/task/wiredtiger_amazon2023_arm64_asan_format_stress_test_disagg_switch_data_validation_stepdown_async_1_patch_ec83e71b7408d78418ce00239d370bb5d0a7eaa9_6abda87c900fb90007bd6e5e_26_10_01_00_25_36/logs?execution=0)
is `amazon2023-arm64-asan`, `format-stress-test-disagg-switch-data-validation-stepdown-async-1`,
patch/version `6abda87c900fb90007bd6e5e`, base revision above. It aborted in mirror verification,
not from a timeout. Format repetition 2, reopen 1 was involved.

The ordering, using decimal WT timestamps, is:

| Event | Timestamp | Verified meaning |
|---|---:|---|
| Prior periodic checkpoint | 5775189 | Legitimately predates the delete |
| Victim delete commit | 5918559 | Both base and layered removes succeeded, WT txn 714009 |
| Oldest at stepdown | 6078002 | Already later than the delete |
| Stepdown cutoff/checkpoint | 6089378 | Must include the committed delete |
| Neighbor mirrored insert | 6144118 | Above cutoff; stable and ingest writes both succeeded |
| Step-up checkpoint | 6361164 | Old victim payload remains after ingest drain |

The victim is `0000552900.00/opqrstuvwxyza` in layered t2 (27 bytes); its corresponding base t1 key
is `0000552900.00/opqrstuvwxyzabcd` (30 bytes). Its bulk value is 1442 bytes, CityHash64
`10444987975111998547`. The neighbor is `.03` in layered versus `.01` in base, 177 bytes,
CityHash64 `9621799629612927840`. They are distinct keys; the neighbor does not recreate the victim.

The completed stepdown checkpoint (metadata LSN 3028673, completion LSN 3028675) reaches original
leaf **table 33 / page 231785 / LSN 494571**, containing exactly the live victim and no stop window.
The step-up checkpoint (metadata LSN 3188752, completion LSN 3188754) reaches a new leaf
**page 334386 / LSN 3108395** whose key/value payload equals that old leaf byte-for-byte.
The first post-step-up verification fails at 01:03:43 UTC on 2026-10-01.

Verification covered all 57 regular archive files byte-for-byte, 38 joined transaction scopes,
eight checkpoint-to-victim paths, and 56 checksum/address/page-version checks. The original log is
19,397,308 lines / 6,183,219,673 bytes. CI has global failure/skip counters, but lacks the per-victim
tombstone flags and reconciliation/skip events needed to prove the precise local sequence there.
Format's `rollback-to-stable-begin/end` messages do not prove RTS ran: disagg format bypasses it.

### Candidate fix already tested

[PR #14774](https://github.com/wiredtiger/wiredtiger/pull/14774), head
`bbf69826146770128ad8f4901928cd5d1aed6ade`, changes leader read routing and cursor positioning.
All 12 source hunks in `cur_layered.c` were applied over this instrumentation and tested without
changing the driver. `fail` and `urgent` each still reproduced **20/20** in both Debug and ASan;
the three preventative controls matched **20/20** each. Address reuse, checkpoint skip, and the
extra victim survived persisted reopen. The PR was removed and source restored byte-for-byte.

Run from the repository root, with the existing Python 3.10 `.venv` activated. The validated workspace
was `/home/ubuntu/wt-18724-stepdown-repro`, based on `develop` at
`ec83e71b7408d78418ce00239d370bb5d0a7eaa9`.

## Build and run

```bash
source .venv/bin/activate
cmake -B build_stepdown_repro -G Ninja \
  -DCMAKE_C_COMPILER=/opt/mongodbtoolchain/v5/bin/gcc \
  -DCMAKE_CXX_COMPILER=/opt/mongodbtoolchain/v5/bin/g++ \
  -DPYTHON3_REQUIRED_VERSION=3.10 -DPython3_EXECUTABLE="$PWD/.venv/bin/python" \
  -DCMAKE_BUILD_TYPE=Debug -DHAVE_DIAGNOSTIC=1 \
  -DENABLE_SHARED=ON -DENABLE_PALITE=ON -DENABLE_PYTHON=ON \
  -DENABLE_CPPSUITE=OFF -DENABLE_MODEL=OFF
cmake --build build_stepdown_repro -j 8
python3 tools/stepdown_repro/build.py --build build_stepdown_repro
python3 tools/stepdown_repro/run.py --build build_stepdown_repro \
  --count 100 --jobs 4 --name my-debug-run --quiet
```

Use a new `--name` for every batch. All homes, traces, complete key/value dumps, console logs,
and validation results are retained beneath `tools/stepdown_repro/<name>/`.
Names must be a single directory component and modes must be unique; existing names are refused.
For a quick first run, use `--modes fail --count 1 --jobs 1`.

These commands were validated on Linux ARM64 with MongoDB toolchain v5, CMake/Ninja, PALite and
SQLite. The helper supports diagnostic shared-library Debug and Clang ASan only. It fails early
if the configuration is unsupported. On ARM64 it enables the RCpc/CRC instructions expected by
the engine headers; other hosts need an appropriate toolchain. If `.venv` is absent, create a
Python 3.10 environment at `.venv` before configuring.

The runner exits **0 when the experiment and all validations succeed**, including an expected
mirror mismatch. The standalone driver exits **2 for a reproduced mismatch**, **0 for matching
views**, and **1 for a driver/API error**. Unexpected signals and timeouts fail validation.

### ASan

```bash
source .venv/bin/activate
cmake -B build_stepdown_repro_asan -G Ninja \
  -DCMAKE_C_COMPILER=/opt/mongodbtoolchain/v5/bin/clang \
  -DCMAKE_CXX_COMPILER=/opt/mongodbtoolchain/v5/bin/clang++ \
  -DCMAKE_BUILD_TYPE=ASan -DHAVE_DIAGNOSTIC=1 \
  -DENABLE_SHARED=ON -DENABLE_PALITE=ON -DENABLE_PYTHON=OFF \
  -DENABLE_CPPSUITE=OFF -DENABLE_MODEL=OFF -DENABLE_STRICT=OFF
cmake --build build_stepdown_repro_asan -j 8
python3 tools/stepdown_repro/build.py --build build_stepdown_repro_asan \
  --output tools/stepdown_repro/stepdown-repro-asan
python3 tools/stepdown_repro/run.py --binary tools/stepdown_repro/stepdown-repro-asan \
  --build build_stepdown_repro_asan --count 100 --jobs 4 --name my-asan-run --quiet
```

The compile helper discovers the compiler and ASan runtime path from the configured build.
The driver and WiredTiger/PALite use the same shared ASan runtime. No sanitizer suppression is used.

### Built-in random failpoints

```bash
source .venv/bin/activate
python3 tools/stepdown_repro/run.py --build build_stepdown_repro --modes ci-random --count 1000 --jobs 4 \
  --name my-random-run --quiet
```

This enables `failpoint_rec_before_wrapup` around the first eviction. It does not force the random draw. A run without a failpoint hit
legitimately matches. `builtin_failures` records pre-wrapup EBUSY outcomes.

Earlier runs also requested `failpoint_history_store_delete_key_from_ts`. Review found that this
revision accepts that spelling but its runtime registry uses `failpoint_history_delete_key_from_ts`,
silently leaving that flag disabled. The driver now requests only the functioning pre-wrapup
failpoint. The historical results establish pre-wrapup coverage, not history-store-failpoint coverage.
The generic helper's inclusive comparison makes threshold `100` nominally 1.01%, and `5000` 50.01%.
Neither value forces a failure; the engine RNG is time/PID/session seeded. New traces record its
state immediately before the relevant draw; trial index alone does not replay the random outcome.

The pre-wrapup probability argument is `100` (approximately 1%). The temporary 50% change used
for the experiment below has been reverted.

## Modes

| Mode | Difference from `fail` | Expected result |
|---|---|---|
| `fail` | Targeted pre-wrapup EBUSY after empty-page durability | Mismatch |
| `no-fail` | Disable targeted failure | Match |
| `mirror-off` | Disable stepdown mirroring | Match |
| `no-skip` | Decline the stable-tree checkpoint snapshot skip | Match |
| `urgent` | Add the urgent flag to the internal eviction session | Mismatch |
| `ci-random` | Use engine random failpoints instead of targeted EBUSY | Depends on hit |

Urgent internal-session eviction is not equivalent to application-thread urgent eviction.

### Testing a proposed fix

Default runner success means the known failure and controls behaved as expected. To validate a
candidate fix without disabling the original trigger, rebuild engine and driver, then use:

```bash
source .venv/bin/activate
cmake --build build_stepdown_repro -j 8
python3 tools/stepdown_repro/build.py --build build_stepdown_repro
python3 tools/stepdown_repro/run.py --build build_stepdown_repro \
  --modes fail,urgent --expect-match --count 100 --jobs 4 --name candidate-fix --quiet
```

`--expect-match` changes the expectation, not the workload or injected failure. It still requires
the targeted EBUSY, complete raw key/value views, correct cutoff contents, clean close, and
agreement after reopen. Re-run the other controls separately. If a proposed fix removes the early
durability event that arms our callback, this targeted mode cannot establish its trigger; use
`ci-random --expect-match`, retain actual failpoint opportunities/hits, and inspect the changed path.
Do not count a passing trial in which the trigger was never reached as evidence that the bug is fixed.

The build helper first brings the engine and PALite targets up to date, then writes
`stepdown-repro.build.json` with source, library, PALite, driver and config
hashes, compiler/version, and probability. The runner checks those hashes before execution and
retains `manifest.json` plus provenance in `results.json`; it also checks dependency hashes after
the batch. Do not rebuild or edit source during a running batch.

## End-to-end checks

1. Seed the exact raw victim key and 1442-byte value; checkpoint, demote, close, reopen, and step up.
2. Commit the delete during a paused periodic checkpoint, then schedule actual engine eviction.
3. Publish the cutoff, commit the 177-byte mirrored neighbor, and run the stepdown checkpoint.
4. Read the published stable checkpoint independently before demotion.
5. Demote, deliver checkpoint metadata, and compare complete base/layered views as follower.
6. Step up, drain ingest, checkpoint, and compare complete views and the stable checkpoint.
7. Demote before closing to preserve the checkpoint under test; reopen as follower and pick up it.
8. Compare every key and value again, inspect the loaded stable constituent, and close cleanly.

The validator requires exact fixture values, ordered unique keys, correct cutoff contents,
consistent retained page LSN/cookie, and a complete final `closed` trace event. It preserves
repeated trace fields as lists rather than silently discarding earlier update/product entries.
The only permitted mismatch is the original victim `0000552900.00` in the layered view.
Both views retain the neighbor, and all other values agree after table-key normalization.
Raw table-specific keys are checked before reporting normalized identities. API errors cannot
stand in for `WT_NOTFOUND`. Each random trial must reach one relevant stable-page pre-wrapup
outcome; new batches also require its opportunity/probability/RNG event.

### Reading a failing run

Start with `<batch>/fail-0000/console.log` and `validation.json`. The expected output has
`first_eviction=16 injected_failures=1`, `second_eviction=0`, and `RESULT ... mismatch=1`;
victim search is `base=-31803 layered=0` at follower, step-up and persisted stages. Dump files
are TSV pairs of hexadecimal **complete raw key and value bytes**, not human-readable strings.

In `repro.trace`, follow `empty-durable-after` → targeted `rec-before-wrapup` →
`rec-error-cleaned` → `reuse-address` → `restore-complete` → `checkpoint-skip`. The reused
reference cookie and page LSN must match the skip's; the skip carries current `rec_stamp`, while
the restored in-memory image may have zero rows. `stepdown-stable-checkpoint.tsv` independently
shows that the stored victim remains. The `.03` neighbor is absent from the cutoff image but
appears after ingest drain in `stepup-stable-checkpoint.tsv` and persisted data.

Trace fields are individually sampled, not an atomic global snapshot. `has_snapshot=0` means
the printed bounds are cached/inactive. Repeated fields are retained in emission order; product
and saved-entry indices delimit those groups, and `insert_key_hash` is distinct from payload hash.
`completed_meta` ties a checkpoint timestamp to metadata LSN; pickup is checked against publication.
The immediate post-demotion same-LSN pickup may be a no-op; the fresh follower reopen adopts it.

The full-chain logger is specific to this tiny, gated single-writer fixture. A page pin alone is
not a general guarantee that obsolete update tails/products are safe to traverse concurrently.
Do not install this callback into a format worker pool or an arbitrary live workload without
redesigning capture around engine locks and update lifetime. Hooks with a NULL callback have a
cheap conditional path; enabled tracing deliberately perturbs timing and is not for benchmarking.

## Reproducer repairs

- **Trace crash:** the driver's pin trace ran without a current data handle, while address-cell
  unpacking needs `S2BT(session)->base_write_gen`. The saved core proves this call chain.
  The trace now runs within the handle scope and address decoding requires a B-tree handle.
- **Reference lifetime:** retain a split generation across reset/eviction; don't dereference the
  reference after successful eviction. Log restoration before publishing the reference unlocked.
- **Checkpoint persistence:** a leader close creates another checkpoint and can heal the mismatch.
  Demote before closing so reopen checks the intended image.
- **Follower inspection:** connection-level `readonly=true` is rejected by this revision. A fresh
  follower is used instead. Opening a named checkpoint after pickup spun in the checkpoint-handle
  open retry path; a separate ordinary file cursor returned EBUSY. Inspect the already-loaded stable
  constituent owned by the layered cursor instead.
- **Random configuration:** use the engine's history-store failpoint name, not the format option name.
- **ASan runtime:** use the shared runtime for both the driver and library and embed its search path.
- **Runner failures:** retain incremental results and per-home validation even after a crash/timeout.

## Scope

This is a deterministic, scheduled reproducer using the real eviction, snapshot, reconciliation,
checkpoint, pickup, and ingest paths. It does not assign durability flags, checkpoint stamps,
addresses, or reconciliation products. Scheduling gates and targeted failures are traced explicitly.
It uses a small fixture, two tables, and no format worker pool. `ci-random` still schedules eviction;
natural cache-pressure eviction is not implemented. This demonstrates the local mechanism and
does not establish every transient event in the original CI execution. No product fix is applied.

Historical `final-debug/`, `final-asan/`, and `final-random/` batches remain in the original
workspace `/home/ubuntu/wt-18724-stepdown-repro/.vscode/repro/`, not in this checkout. Generated
artifacts are gitignored and are **not transferred by cloning this branch**.

## Evidence locations and next work

These local paths were retained on the investigation host; obtain an archive/share from the handing-off
engineer if using another host. The README and source travel with the branch, the multi-GB data do not.

| Host-local path | Contents |
|---|---|
| `~/wiredtiger/.vscode/wt-18724/patch-3543-amazon2023-arm64-asan-1-timeline.md` | Detailed CI timeline and root-to-leaf proof |
| `~/wiredtiger/.vscode/tmp/patch3543-asan1/` | Full RUNDIR archive, log, PALite database/WAL, checksum/path validation outputs |
| `~/wiredtiger/.vscode/wt-18724/stepdown-mirroring-deep-dive-notes.md` | Earlier source investigation and hypotheses |
| `~/wt-18724-stepdown-repro/.vscode/repro/` | Original 2000-run evidence, PR14774 runs and `pr14774-experiment/RESULT.md` |
| `tools/stepdown_repro/ci-random-50pct/` | Relocated 1000-trial 50% experiment; source change reverted |
| `tools/stepdown_repro/relocated-validation/` | Before-review, 20 runs per mode after migration |

Inspect original CI databases read-only and preferably on copies. Read-only SQLite can still
change transient SHM bookkeeping; the archive originals were restored and revalidated. Avoid
leader close/checkpoint on evidence databases: it can heal the stale-image mismatch.

Suggested next steps for the receiving engineer:

1. Run the deterministic baseline and controls; inspect one full event chain and persisted dump.
2. Develop the reconciliation durability/failure fix, using the source contracts above, and validate
   with the original failure schedule and `--expect-match` (or random hits if its arm event changes).
3. Add an appropriate regression test and check other empty-page/delta/restore paths.
4. Relax explicit eviction/checkpoint scheduling toward natural cache-pressure/format execution.
5. Add victim-level transient evidence in CI before claiming this mechanism caused that exact run.

Earlier mirror-off patch #3542 passed all 28 tasks, and multiple #3541 failures also showed
pre-cutoff committed deletes followed by stale rows. Those support investigation direction but
do not replace the victim-level CI proof still needed. A public commit/stepdown-lock gap was
observed separately; it does not explain the already-completed pre-cutoff delete described here.

## Verified results (2026-10-01)

| Mode | Debug mismatches/runs | ASan mismatches/runs | Validation errors |
|---|---:|---:|---:|
| `fail` | 100/100 | 100/100 | 0 |
| `no-fail` | 0/100 | 0/100 | 0 |
| `mirror-off` | 0/100 | 0/100 | 0 |
| `no-skip` | 0/100 | 0/100 | 0 |
| `urgent` | 100/100 | 100/100 | 0 |
| `ci-random` | 8/1000 | Not run in the final random batch | 0 |

The 8 random mismatches correspond exactly to the 8 built-in pre-wrapup failures.
All 2000 runs validated complete logical and persisted views and reached clean close.
ASan ran with default leak checking and no suppressions; all 500 runs were clean.
An intentionally substituted empty persisted view was rejected by the validator.
A deliberately failing subprocess also produced a retained report and a nonzero runner exit.

## 50% probability experiment

After changing `WT_TIMING_STRESS_FAILPOINT_REC_BEFORE_WRAPUP` from `100` to `5000` and rebuilding
in `build_stepdown_repro`, `ci-random-50pct/` completed 1000 fresh trials:

- 491 built-in pre-wrapup failures and 491 persisted mirror mismatches (49.1%).
- 509 matching trials; zero unexpected errors.
- Each trial had one relevant stable-page pre-wrapup outcome. No custom failure intervention ran,
  and the built-in failure outcome equalled the mismatch outcome in all 1000 trials.
- The helper uses `random % 10000 <= probability`, giving nominal probability 50.01% at `5000`.
  Expected hits: 500.1; central 95% binomial count range: 469-531.
- Observed-rate Wilson 95% interval: 46.01%-52.20%; exact two-sided binomial p-value: 0.5694.
  The observed rate is consistent with the configured probability.

Results and full traces: `ci-random-50pct/results.json` and its per-trial homes.
Statistical/trace verification: `ci-random-50pct/probability-analysis.json`.
Re-run analysis with
`python tools/stepdown_repro/analyze_probability.py tools/stepdown_repro/ci-random-50pct`.
It writes `probability-reanalysis.json`, preserving the original analysis. New batches use captured
manifest probability and hashes. Legacy batches use existing analysis-recorded metadata, explicitly
labelled as unauthenticated legacy provenance, or require `--probability` if it is absent. No current
source/library hashes are substituted for historical execution artifacts. Explicit checks remain
active under Python optimization.

## Pre-commit review verification

The handoff review covered correctness, concurrency, error cleanup, assertions, API contracts,
disagg, performance, logging, style, test validation, and relevant git history. It strengthened
raw-key/API-error validation, added candidate-fix expectations, recorded build provenance and
random opportunity/RNG state, moved callback registration before worker startup, and corrected
shared scalar sampling and missing trace identities. The failpoint probability remains 1.01%.

`handoff-debug/` and `handoff-asan/` each completed 100 trials per deterministic mode with zero
unexpected errors; `fail` and `urgent` reproduced 100/100 each, and each preventative control
matched 100/100. The strengthened random batch `handoff-random/` had 7/1000 pre-wrapup hits and
7/1000 mismatches. After final formatting and provenance checks, `handoff-final-debug/` and
`handoff-final-asan/` each completed 20 trials per mode without errors. Generated results remain
host-local, not in the commit.

Targeted copyright, clang-format, spelling, Python syntax, Ruff, and diff-whitespace checks passed.
Full `dist/s_fast -E --no-interactive` is not clean on this host: `s_export` recursively examines
existing ASan libraries and rejects their compiler-generated `__odr_asan_gen_*` symbols. That
check is separate from passing driver/engine builds and clean ASan repro runs. README ticket
references also produce `s_mentions` advisory output; they are intentional handoff context.
