# WT-14196 Compact Performance Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend wtperf with concurrent foreground compact + a post-populate fragmentation pass, wire it into the Evergreen perf pipeline as a permanent benchmark for WT-14196 compact-under-load regressions and solution evaluation.

**Architecture:** Add new options, structs, and three new threads (compact_worker, stats_sampler_worker, plus a non-thread `execute_post_populate_remove` pass) to wtperf. The compact thread fires `session->compact()` once during the workload phase, snapshots TRACK update aggregates and `statistics:URI` cursor values pre/post, and writes `compact_summary.txt`. A stats sampler tracks the peak `block_first_srch_walk_time`. A new `PerfStatLatencyPercentile` class in `perf_run_py` reads `latency.update` for p50/p95/p99/p99.9.

**Tech Stack:** C (wtperf), Python 3 (perf_run_py), wtperf config format, Evergreen YAML.

**Spec:** `dev-notes/specs/2026-05-11-wt14196-compact-perf-test-design.md`

**Working directory for execution:** `/home/ubuntu/wiredtiger-sean-compact-perf` (worktree on branch `sean-compact-perf`).

---

## Build & test prerequisites

Before any task, the worker should have a working CMake build. One-time setup from the worktree root:

```bash
cd /home/ubuntu/wiredtiger-sean-compact-perf
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DENABLE_PYTHON=0 -DHAVE_DIAGNOSTIC=0
cmake --build build --target wtperf -j$(nproc)
```

Expected: `build/bench/wtperf/wtperf` exists and `./build/bench/wtperf/wtperf -h` prints usage.

Each subsequent task that touches wtperf C code rebuilds with `cmake --build build --target wtperf -j$(nproc)`.

---

## Task 1: Add new wtperf options

**Files:**
- Modify: `bench/wtperf/wtperf_opt_inline.h:98` (before the existing `compact` option)

The new options must default to disabled (0/empty string) so existing wtperf runners are unaffected.

- [ ] **Step 1: Add options block**

Open `bench/wtperf/wtperf_opt_inline.h` and locate the existing line:

```c
DEF_OPT_AS_BOOL(compact, 0, "post-populate compact")
```

Replace it with the existing line plus the six new options immediately after, keeping options sorted alphabetically-ish per the file's existing convention:

```c
DEF_OPT_AS_BOOL(compact, 0, "post-populate compact")
DEF_OPT_AS_BOOL(compact_ends_workload, 1,
  "when a compact_worker returns, signal worker threads to stop. If false, workload runs "
  "until run_time elapses even after compact finishes.")
DEF_OPT_AS_UINT32(compact_start_after, 0,
  "seconds into the workload phase to wait before issuing compact, 0 fires immediately.")
DEF_OPT_AS_UINT32(compact_threads, 0,
  "number of foreground compact threads. Each runs session->compact() once "
  "after compact_start_after seconds into the workload phase. Independent of "
  "the (post-populate) compact option above.")
DEF_OPT_AS_STRING(compact_uri, "",
  "URI for compact_threads to operate on. Empty means the first table.")
DEF_OPT_AS_UINT32(post_populate_remove_modulus, 0,
  "after populate, remove every Nth key to create fragmentation. "
  "0 disables. Takes precedence over post_populate_remove_pct.")
DEF_OPT_AS_UINT32(post_populate_remove_pct, 0,
  "after populate, remove this percentage of randomly-chosen keys. "
  "0 disables. Ignored if post_populate_remove_modulus is set.")
```

- [ ] **Step 2: Rebuild wtperf**

```bash
cmake --build build --target wtperf -j$(nproc)
```

Expected: clean build. The `OPT_DECLARE_STRUCT`/`OPT_DEFINE_DESC`/`OPT_DEFINE_DEFAULT` macros in the header auto-wire the new fields into `CONFIG_OPTS` so no other code change is required for parsing.

- [ ] **Step 3: Smoke test that the new options parse**

```bash
./build/bench/wtperf/wtperf -O bench/wtperf/runners/small-btree.wtperf \
   -o compact_threads=1 -o post_populate_remove_modulus=3 -o run_time=1
```

Expected: wtperf starts, runs briefly, exits cleanly. The new options are accepted (no "unknown option" error). The compact thread isn't actually started yet — that's later — but options must parse without error. Errors like `Configuration for 'compact_threads' not supported` indicate a typo or build issue.

Tidy up the test home:

```bash
rm -rf build/bench/wtperf/WT_TEST
```

- [ ] **Step 4: Commit**

```bash
cd /home/ubuntu/wiredtiger-sean-compact-perf
git add bench/wtperf/wtperf_opt_inline.h
git commit -m "WT-14196 wtperf: add compact_threads and post_populate_remove options"
```

---

## Task 2: Add wtperf data structures

**Files:**
- Modify: `bench/wtperf/wtperf.h` (struct `__wtperf` around line 145, struct fields)
- Modify: `bench/wtperf/wtperf.h` (TRACK list in `__wtperf_thread`)

We need new state on the WTPERF struct for the compact thread to communicate results to the end-of-run summary writer, and a new TRACK on the per-thread struct for compact wallclock timing.

- [ ] **Step 1: Add fields to `__wtperf`**

In `bench/wtperf/wtperf.h`, after the existing `WTPERF_THREAD *scanthreads;` line (currently around line 168):

```c
    WTPERF_THREAD *compactthreads;        /* Foreground compact threads */
    WTPERF_THREAD *statssamplerthreads;   /* Stats sampler threads */
```

Then, in the "State tracking variables" block after `uint64_t update_ops;` (currently around line 186), add a new "Compact benchmark state" block:

```c
    /* Compact benchmark state (WT-14196). */
    volatile bool compact_done;            /* Compact returned; sampler should stop */
    int compact_ret;                       /* Return code from session->compact() */
    uint64_t compact_wallclock_us;         /* session->compact() wallclock in usecs */
    uint64_t compact_pre_file_size;        /* file size before compact (bytes) */
    uint64_t compact_post_file_size;       /* file size after compact (bytes) */
    uint64_t compact_pre_reuse_bytes;      /* block_reuse_bytes before compact */
    uint64_t compact_post_reuse_bytes;     /* block_reuse_bytes after compact */
    uint64_t compact_pages_reviewed;       /* btree_compact_pages_reviewed final */
    uint64_t compact_pages_rewritten;      /* btree_compact_pages_rewritten final */
    uint64_t compact_pages_skipped;        /* btree_compact_pages_skipped final */
    uint64_t block_first_srch_walk_peak_us;/* peak observed during compact */
    /* Update TRACK aggregates snapshot at compact start and end. */
    uint64_t update_ops_pre_compact;
    uint64_t update_ops_post_compact;
    uint64_t update_latency_pre_compact;   /* cumulative latency_ops snapshot */
    uint64_t update_latency_post_compact;
    uint64_t update_max_latency_during_compact_us;
    /* Post-populate remove state (WT-14196). */
    uint64_t post_populate_remove_records;
    uint64_t post_populate_remove_us;      /* wallclock duration in usecs */
```

- [ ] **Step 2: Add TRACK to `__wtperf_thread`**

The per-thread struct is around line 279. Find the existing block:

```c
    TRACK update;         /* Update operations */
```

Replace with:

```c
    TRACK update;         /* Update operations */
    TRACK compact;        /* Compact operations (foreground compact thread only) */
```

This gives the compact thread a TRACK entry so `latency_op`-style code can report compact wallclock if we ever want a histogram. For v1 we use the simpler `compact_wallclock_us` scalar directly.

- [ ] **Step 3: Rebuild**

```bash
cmake --build build --target wtperf -j$(nproc)
```

Expected: clean build. Struct fields are zero-initialized via the existing `dcalloc`/`memset` paths in `setup_wtperf` and per-thread allocation.

- [ ] **Step 4: Commit**

```bash
git add bench/wtperf/wtperf.h
git commit -m "WT-14196 wtperf: add compact benchmark state and TRACK"
```

---

## Task 3: Implement and wire `execute_post_populate_remove`

**Files:**
- Modify: `bench/wtperf/wtperf.c` (add new static function near `execute_populate`)
- Modify: `bench/wtperf/wtperf.c:38` (forward declaration block)
- Modify: `bench/wtperf/wtperf.c:2453` (call site, after `execute_populate`)

This pass runs single-threaded between populate and the workload phase. Deterministic when modulus is set.

- [ ] **Step 1: Add forward declaration**

In `bench/wtperf/wtperf.c`, near line 38 with the other `static int execute_*` declarations, add:

```c
static int execute_post_populate_remove(WTPERF *);
```

- [ ] **Step 2: Implement the function**

Add after `execute_populate` (which ends around line 1820). Paste the whole function:

```c
/*
 * execute_post_populate_remove --
 *     After populate, remove a deterministic or random subset of keys to fragment
 *     the on-disk extent list. WT-14196.
 */
static int
execute_post_populate_remove(WTPERF *wtperf)
{
    CONFIG_OPTS *opts;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    WT_RAND_STATE rnd;
    uint64_t i, removed;
    uint64_t start_clock;
    uint32_t modulus, pct;
    int ret;

    opts = wtperf->opts;
    modulus = opts->post_populate_remove_modulus;
    pct = opts->post_populate_remove_pct;
    conn = wtperf->conn;

    if (modulus == 0 && pct == 0)
        return (0);
    if (modulus != 0 && pct != 0)
        lprintf(wtperf, 0, 1,
          "post_populate_remove: both modulus and pct set; modulus wins, ignoring pct=%u", pct);

    lprintf(wtperf, 0, 1, "Post-populate remove starting (modulus=%u pct=%u, icount=%" PRIu32 ")",
      modulus, pct, opts->icount);

    if ((ret = conn->open_session(conn, NULL, opts->sess_config, &session)) != 0) {
        lprintf(wtperf, ret, 0, "post_populate_remove: open_session");
        return (ret);
    }
    if ((ret = session->open_cursor(session, wtperf->uris[0], NULL, NULL, &cursor)) != 0) {
        lprintf(wtperf, ret, 0, "post_populate_remove: open_cursor on %s", wtperf->uris[0]);
        (void)session->close(session, NULL);
        return (ret);
    }

    __wt_random_init_seed((WT_SESSION_IMPL *)session, &rnd);
    uint64_t start_clock = __wt_clock(NULL);
    removed = 0;
    for (i = 0; i < opts->icount; ++i) {
        bool del;
        if (modulus != 0)
            del = (i % modulus == 0);
        else
            del = ((__wt_random(&rnd) % 100) < pct);
        if (!del)
            continue;
        cursor->set_key(cursor, i + 1);
        if ((ret = cursor->remove(cursor)) != 0 && ret != WT_NOTFOUND) {
            lprintf(wtperf, ret, 0, "post_populate_remove: remove key %" PRIu64, i + 1);
            goto err;
        }
        ret = 0;
        ++removed;
    }

    /* Checkpoint to materialise the removes on disk. */
    if ((ret = session->checkpoint(session, NULL)) != 0) {
        lprintf(wtperf, ret, 0, "post_populate_remove: checkpoint");
        goto err;
    }

    wtperf->post_populate_remove_us = WT_CLOCKDIFF_US(__wt_clock(NULL), start_clock);
    wtperf->post_populate_remove_records = removed;
    lprintf(wtperf, 0, 1,
      "Post-populate remove finished: removed %" PRIu64 " records in %.2f sec",
      removed, wtperf->post_populate_remove_us / 1.0e6);

err:
    (void)cursor->close(cursor);
    (void)session->close(session, NULL);
    return (ret);
}
```

Note on timing: wtperf uses `__wt_clock(NULL)` to get a clock tick and `WT_CLOCKDIFF_US(stop, start)` to get a microsecond delta. See `wtperf.c` lines 515 and 795 for the canonical idiom in the existing `worker` function.

- [ ] **Step 3: Wire the call into `main`**

Around line 2453 in `bench/wtperf/wtperf.c`, find:

```c
    if (opts->create != 0 && execute_populate(wtperf) != 0)
        goto err;

    /* Optional workload. */
```

Replace with:

```c
    if (opts->create != 0 && execute_populate(wtperf) != 0)
        goto err;

    /* Post-populate fragmentation pass (WT-14196). No-op when both knobs are 0. */
    if (opts->create != 0 && execute_post_populate_remove(wtperf) != 0)
        goto err;

    /* Optional workload. */
```

- [ ] **Step 4: Rebuild and smoke test with a tiny remove pass**

```bash
cmake --build build --target wtperf -j$(nproc)
./build/bench/wtperf/wtperf -O bench/wtperf/runners/small-btree.wtperf \
    -o post_populate_remove_modulus=3 -o run_time=1
```

Expected: log lines `Post-populate remove starting` and `Post-populate remove finished: removed N records ...` appear. wtperf exits with code 0.

```bash
rm -rf build/bench/wtperf/WT_TEST
```

- [ ] **Step 5: Commit**

```bash
git add bench/wtperf/wtperf.c
git commit -m "WT-14196 wtperf: implement post-populate fragmentation pass"
```

---

## Task 4: Implement `compact_worker` thread (no stats sampler yet)

**Files:**
- Modify: `bench/wtperf/wtperf.c` (add new thread function, modelled on `checkpoint_worker`)
- Modify: `bench/wtperf/wtperf.c:38` (forward decls)

This is the simplest version: fires `session->compact()` once, snapshots wallclock, sets `compact_done`, optionally sets `wtperf->stop`. Stats capture is added in Task 5.

- [ ] **Step 1: Add forward declaration**

Near the other `static WT_THREAD_RET` declarations (search for `checkpoint_worker` to find the area), add:

```c
static WT_THREAD_RET compact_worker(void *);
```

- [ ] **Step 2: Implement the worker**

Add after `checkpoint_worker` (find it by `grep -n '^checkpoint_worker' bench/wtperf/wtperf.c`). Paste:

```c
/*
 * compact_worker --
 *     Foreground compact thread for WT-14196 benchmark. Fires session->compact()
 *     once and signals the workload to wind down.
 */
static WT_THREAD_RET
compact_worker(void *arg)
{
    CONFIG_OPTS *opts;
    WTPERF *wtperf;
    WTPERF_THREAD *thread;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    const char *uri;
    uint64_t start_clock, elapsed_us, delay_remaining;
    int ret;

    thread = (WTPERF_THREAD *)arg;
    wtperf = thread->wtperf;
    opts = wtperf->opts;
    conn = wtperf->conn;
    session = NULL;
    ret = 0;

    if ((ret = conn->open_session(conn, NULL, opts->sess_config, &session)) != 0) {
        lprintf(wtperf, ret, 0, "compact_worker: open_session");
        goto err;
    }

    uri = (opts->compact_uri != NULL && opts->compact_uri[0] != '\0') ?
      opts->compact_uri : wtperf->uris[0];

    /* Optional warm-up delay before issuing compact, polling stop. */
    delay_remaining = opts->compact_start_after;
    while (delay_remaining > 0 && !wtperf->stop) {
        sleep(1);
        --delay_remaining;
    }
    if (wtperf->stop)
        goto err;

    lprintf(wtperf, 0, 1, "compact_worker: starting session->compact(%s)", uri);
    start_clock = __wt_clock(NULL);
    ret = session->compact(session, uri, NULL);
    elapsed_us = WT_CLOCKDIFF_US(__wt_clock(NULL), start_clock);

    wtperf->compact_ret = ret;
    wtperf->compact_wallclock_us = elapsed_us;
    if (ret != 0)
        lprintf(wtperf, ret, 0, "compact_worker: session->compact returned %d", ret);

    lprintf(wtperf, 0, 1,
      "compact_worker: session->compact finished in %.2f sec (ret=%d)",
      elapsed_us / 1.0e6, ret);

    wtperf->compact_done = true;
    if (opts->compact_ends_workload)
        wtperf->stop = true;

err:
    if (session != NULL)
        (void)session->close(session, NULL);
    return (WT_THREAD_RET_VALUE);
}
```

The `__wt_clock(NULL)` / `WT_CLOCKDIFF_US(stop, start)` pattern matches existing wtperf code (see `worker` in `wtperf.c` around lines 515 and 795).

- [ ] **Step 3: Wire the compact thread into `execute_workload`'s caller**

The checkpoint thread is started right before `execute_workload(wtperf)` is called. Find the block around line 2475-2480:

```c
        /* Start the checkpoint thread. */
        if (opts->checkpoint_threads != 0) {
            lprintf(...)
            wtperf->ckptthreads = dcalloc(opts->checkpoint_threads, sizeof(WTPERF_THREAD));
            start_threads(
              wtperf, NULL, wtperf->ckptthreads, opts->checkpoint_threads, checkpoint_worker);
        }
```

Immediately after the checkpoint block, add:

```c
        /* Start the foreground compact thread (WT-14196). */
        if (opts->compact_threads != 0) {
            lprintf(wtperf, 0, 1,
              "Starting %" PRIu32 " compact thread(s)", opts->compact_threads);
            wtperf->compactthreads = dcalloc(opts->compact_threads, sizeof(WTPERF_THREAD));
            start_threads(
              wtperf, NULL, wtperf->compactthreads, opts->compact_threads, compact_worker);
        }
```

- [ ] **Step 4: Wire the shutdown**

Find the existing stop/free block for `ckptthreads` (search for `wtperf->ckptthreads != NULL`). After the existing ckpt cleanup:

```c
    if (wtperf->ckptthreads != NULL) {
        stop_threads(1, wtperf->ckptthreads);
        free(wtperf->ckptthreads);
        wtperf->ckptthreads = NULL;
    }
```

add the symmetric block:

```c
    if (wtperf->compactthreads != NULL) {
        stop_threads(opts->compact_threads, wtperf->compactthreads);
        free(wtperf->compactthreads);
        wtperf->compactthreads = NULL;
    }
```

There are usually two such blocks in wtperf.c — one in `execute_workload`'s cleanup path and one in `start_all_runs`'s. Add the matching cleanup in both. Verify with `grep -n 'wtperf->ckptthreads != NULL' bench/wtperf/wtperf.c`.

- [ ] **Step 5: Rebuild + integration smoke test**

```bash
cmake --build build --target wtperf -j$(nproc)
./build/bench/wtperf/wtperf -O bench/wtperf/runners/small-btree.wtperf \
    -o compact_threads=1 -o post_populate_remove_modulus=3 \
    -o "threads=((count=1,updates=1))" -o run_time=30
```

Expected output includes:
- `Post-populate remove finished: removed N records ...`
- `Starting 1 compact thread(s)`
- `compact_worker: starting session->compact(...)`
- `compact_worker: session->compact finished in X.XX sec (ret=0)`
- Process exits cleanly within ~30 seconds (`compact_ends_workload=1` default cuts the workload short).

```bash
rm -rf build/bench/wtperf/WT_TEST
```

- [ ] **Step 6: Commit**

```bash
git add bench/wtperf/wtperf.c
git commit -m "WT-14196 wtperf: add foreground compact_worker thread"
```

---

## Task 5: Capture pre/post compact stats and `block_first_srch_walk_time` peak

**Files:**
- Modify: `bench/wtperf/wtperf.c` (new `capture_*_stats` helpers + `stats_sampler_worker`, wire into `compact_worker`)

Pre-compact and post-compact reads of `statistics:URI` give us file size, block reuse bytes, and compact page counters. A periodic sampler tracks the peak `block_first_srch_walk_time` from connection stats.

- [ ] **Step 1: Add forward declarations**

Near the other static decls:

```c
static WT_THREAD_RET stats_sampler_worker(void *);
static int           capture_pre_compact_stats(WTPERF *, WT_SESSION *, const char *);
static int           capture_post_compact_stats(WTPERF *, WT_SESSION *, const char *);
```

- [ ] **Step 2: Helper that reads a single stat from a statistics:URI cursor**

Add as a static helper near the top of the compact section in wtperf.c:

```c
/*
 * stat_read_u64 --
 *     Read a single uint64 stat from an open statistics cursor.
 */
static int
stat_read_u64(WT_CURSOR *cursor, int stat_key, uint64_t *valuep)
{
    const char *desc, *str_val;
    int ret;

    cursor->set_key(cursor, stat_key);
    if ((ret = cursor->search(cursor)) != 0)
        return (ret);
    return (cursor->get_value(cursor, &desc, &str_val, valuep));
}
```

- [ ] **Step 3: Pre-compact capture**

```c
/*
 * capture_pre_compact_stats --
 *     Read file size, block_reuse_bytes, and compact page counters before
 *     compact starts.
 */
static int
capture_pre_compact_stats(WTPERF *wtperf, WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    char stat_uri[256];
    int ret;

    testutil_snprintf(stat_uri, sizeof(stat_uri), "statistics:%s", uri);
    if ((ret = session->open_cursor(session, stat_uri, NULL, "statistics=(all)", &cursor)) != 0) {
        lprintf(wtperf, ret, 0, "capture_pre_compact_stats: open_cursor %s", stat_uri);
        return (ret);
    }
    (void)stat_read_u64(cursor, WT_STAT_DSRC_BLOCK_SIZE, &wtperf->compact_pre_file_size);
    (void)stat_read_u64(cursor, WT_STAT_DSRC_BLOCK_REUSE_BYTES, &wtperf->compact_pre_reuse_bytes);
    return (cursor->close(cursor));
}
```

- [ ] **Step 4: Post-compact capture**

```c
/*
 * capture_post_compact_stats --
 *     Read final compact page counters, file size, and block_reuse_bytes.
 */
static int
capture_post_compact_stats(WTPERF *wtperf, WT_SESSION *session, const char *uri)
{
    WT_CURSOR *cursor;
    char stat_uri[256];
    int ret;

    testutil_snprintf(stat_uri, sizeof(stat_uri), "statistics:%s", uri);
    if ((ret = session->open_cursor(session, stat_uri, NULL, "statistics=(all)", &cursor)) != 0) {
        lprintf(wtperf, ret, 0, "capture_post_compact_stats: open_cursor %s", stat_uri);
        return (ret);
    }
    (void)stat_read_u64(cursor, WT_STAT_DSRC_BLOCK_SIZE, &wtperf->compact_post_file_size);
    (void)stat_read_u64(cursor, WT_STAT_DSRC_BLOCK_REUSE_BYTES, &wtperf->compact_post_reuse_bytes);
    (void)stat_read_u64(cursor, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REVIEWED,
                        &wtperf->compact_pages_reviewed);
    (void)stat_read_u64(cursor, WT_STAT_DSRC_BTREE_COMPACT_PAGES_REWRITTEN,
                        &wtperf->compact_pages_rewritten);
    (void)stat_read_u64(cursor, WT_STAT_DSRC_BTREE_COMPACT_PAGES_SKIPPED,
                        &wtperf->compact_pages_skipped);
    return (cursor->close(cursor));
}
```

Note: confirmed all stat enum names exist in `src/include/wiredtiger.h.in`. `session_table_compact_passes` is intentionally omitted — it's a session-level internal stat that isn't exposed via the public `statistics:` cursor, so we drop it from the metrics. Spec note updated to match.

- [ ] **Step 5: Implement the stats sampler thread**

```c
/*
 * stats_sampler_worker --
 *     While compact is running, sample the connection block_first_srch_walk_time
 *     stat every 100 ms and track the max. Exits when wtperf->compact_done is set.
 */
static WT_THREAD_RET
stats_sampler_worker(void *arg)
{
    WTPERF *wtperf;
    WTPERF_THREAD *thread;
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_CURSOR *cursor;
    const char *desc, *str_val;
    uint64_t sample, peak;
    int ret;

    thread = (WTPERF_THREAD *)arg;
    wtperf = thread->wtperf;
    conn = wtperf->conn;
    session = NULL;
    cursor = NULL;
    peak = 0;

    if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0) {
        lprintf(wtperf, ret, 0, "stats_sampler: open_session");
        goto done;
    }
    if ((ret = session->open_cursor(
           session, "statistics:", NULL, "statistics=(all)", &cursor)) != 0) {
        lprintf(wtperf, ret, 0, "stats_sampler: open_cursor statistics:");
        goto done;
    }

    while (!wtperf->compact_done && !wtperf->stop) {
        cursor->set_key(cursor, WT_STAT_CONN_BLOCK_FIRST_SRCH_WALK_TIME);
        if ((ret = cursor->search(cursor)) == 0) {
            (void)cursor->get_value(cursor, &desc, &str_val, &sample);
            if (sample > peak)
                peak = sample;
        }
        cursor->reset(cursor);
        usleep(100 * 1000); /* 100 ms */
    }
    wtperf->block_first_srch_walk_peak_us = peak;

done:
    if (cursor != NULL)
        (void)cursor->close(cursor);
    if (session != NULL)
        (void)session->close(session, NULL);
    return (WT_THREAD_RET_VALUE);
}
```

- [ ] **Step 6: Hook pre/post capture and sampler launch into `compact_worker`**

In `compact_worker`, between the optional delay and the `__wt_clock(NULL)` start snapshot, add the pre-compact stat capture and launch a sibling sampler thread. Replace the body of `compact_worker` between the delay loop and the post-compact log line with:

```c
    if (wtperf->stop)
        goto err;

    if ((ret = capture_pre_compact_stats(wtperf, session, uri)) != 0)
        goto err;

    /*
     * Launch a stats sampler thread to track the peak
     * block_first_srch_walk_time during compact. It exits when we set
     * wtperf->compact_done below.
     */
    wtperf->statssamplerthreads = dcalloc(1, sizeof(WTPERF_THREAD));
    start_threads(wtperf, NULL, wtperf->statssamplerthreads, 1, stats_sampler_worker);

    lprintf(wtperf, 0, 1, "compact_worker: starting session->compact(%s)", uri);
    start_clock = __wt_clock(NULL);
    ret = session->compact(session, uri, NULL);
    elapsed_us = WT_CLOCKDIFF_US(__wt_clock(NULL), start_clock);

    wtperf->compact_ret = ret;
    wtperf->compact_wallclock_us = elapsed_us;
    if (ret != 0)
        lprintf(wtperf, ret, 0, "compact_worker: session->compact returned %d", ret);

    (void)capture_post_compact_stats(wtperf, session, uri);

    lprintf(wtperf, 0, 1,
      "compact_worker: session->compact finished in %.2f sec (ret=%d)",
      elapsed_us / 1.0e6, ret);

    wtperf->compact_done = true;

    /* Join the sampler thread before flipping wtperf->stop. */
    if (wtperf->statssamplerthreads != NULL) {
        stop_threads(1, wtperf->statssamplerthreads);
        free(wtperf->statssamplerthreads);
        wtperf->statssamplerthreads = NULL;
    }

    if (opts->compact_ends_workload)
        wtperf->stop = true;

err:
    if (session != NULL)
        (void)session->close(session, NULL);
    return (WT_THREAD_RET_VALUE);
```

- [ ] **Step 7: Rebuild and verify the sampler runs**

```bash
cmake --build build --target wtperf -j$(nproc)
./build/bench/wtperf/wtperf -O bench/wtperf/runners/small-btree.wtperf \
    -o compact_threads=1 -o post_populate_remove_modulus=3 \
    -o "threads=((count=1,updates=1))" -o run_time=30
```

Expected: clean exit. No crash from the sampler thread. The new field
`block_first_srch_walk_peak_us` is populated (it'll be written to `compact_summary.txt` in Task 6 — for now we just verify the build links cleanly and the binary still runs end-to-end).

```bash
rm -rf build/bench/wtperf/WT_TEST
```

- [ ] **Step 8: Commit**

```bash
git add bench/wtperf/wtperf.c
git commit -m "WT-14196 wtperf: capture pre/post compact stats and walk-time peak"
```

---

## Task 6: TRACK update snapshot and `compact_stats_dump` writer

**Files:**
- Modify: `bench/wtperf/wtperf.c` (helper + dump function + call sites in `compact_worker` and end-of-run)

Snapshot the per-thread update TRACK aggregates at compact start and end so we can report "ops during compact", "avg latency during compact", and "max latency during compact". Then write the human-readable `compact_summary.txt`.

- [ ] **Step 1: Snapshot helper**

Add near the top of the compact section in wtperf.c:

```c
/*
 * snapshot_update_track --
 *     Sum the worker threads' update TRACK ops and latency counters into
 *     scalars. Used at compact start and end to compute deltas.
 */
static void
snapshot_update_track(WTPERF *wtperf, uint64_t *ops, uint64_t *latency_sum, uint64_t *max_us)
{
    WTPERF_THREAD *thread;
    int64_t i;
    uint64_t sum_ops, sum_lat, cur_max;

    sum_ops = sum_lat = 0;
    cur_max = 0;
    for (i = 0, thread = wtperf->workers; thread != NULL && i < wtperf->workers_cnt;
         ++i, ++thread) {
        sum_ops += thread->update.ops;
        sum_lat += thread->update.latency;
        if (thread->update.max_latency > cur_max)
            cur_max = thread->update.max_latency;
    }
    *ops = sum_ops;
    *latency_sum = sum_lat;
    *max_us = cur_max;
}
```

Note: confirm field names by `grep -n 'struct.*TRACK\|^TRACK \|min_latency\|max_latency\|ops;\|latency;' bench/wtperf/wtperf.h`. The TRACK struct uses `ops`, `latency`, and `max_latency` fields (existing names).

- [ ] **Step 2: Wire snapshots into `compact_worker`**

Before the `start_clock = __wt_clock(NULL)` line in `compact_worker`, add:

```c
    {
        uint64_t snap_lat_unused, snap_max_unused;
        snapshot_update_track(wtperf, &wtperf->update_ops_pre_compact,
          &wtperf->update_latency_pre_compact, &snap_max_unused);
        (void)snap_max_unused;
        (void)snap_lat_unused;
    }
```

(Single brace block keeps the helper variables local without disturbing the `goto err` flow.)

After the post-compact stats capture, before `wtperf->compact_done = true`, add:

```c
    {
        uint64_t max_us;
        snapshot_update_track(wtperf, &wtperf->update_ops_post_compact,
          &wtperf->update_latency_post_compact, &max_us);
        wtperf->update_max_latency_during_compact_us = max_us;
    }
```

- [ ] **Step 3: Implement `compact_stats_dump`**

Add at the bottom of wtperf.c near `latency_print`:

```c
/*
 * compact_stats_dump --
 *     Write compact_summary.txt with metrics consumed by perf_run_py.
 *     WT-14196.
 */
static void
compact_stats_dump(WTPERF *wtperf)
{
    CONFIG_OPTS *opts;
    FILE *fp;
    const char *uri;
    char path[1024];
    uint64_t ops_during, lat_sum_during;
    double reduction_pct, avg_lat_us;

    opts = wtperf->opts;

    if (opts->compact_threads == 0)
        return; /* This benchmark wasn't run; don't emit a misleading file. */

    testutil_snprintf(path, sizeof(path), "%s/compact_summary.txt", wtperf->monitor_dir);
    if ((fp = fopen(path, "w")) == NULL) {
        lprintf(wtperf, errno, 0, "compact_stats_dump: fopen %s", path);
        return;
    }

    uri = (opts->compact_uri != NULL && opts->compact_uri[0] != '\0') ?
      opts->compact_uri : wtperf->uris[0];

    ops_during = wtperf->update_ops_post_compact - wtperf->update_ops_pre_compact;
    lat_sum_during = wtperf->update_latency_post_compact - wtperf->update_latency_pre_compact;
    avg_lat_us = ops_during == 0 ? 0.0 : (double)lat_sum_during / (double)ops_during;
    reduction_pct = wtperf->compact_pre_file_size == 0 ? 0.0 :
      100.0 * (double)(wtperf->compact_pre_file_size - wtperf->compact_post_file_size) /
      (double)wtperf->compact_pre_file_size;

    fprintf(fp, "Compact configuration uri : %s\n", uri);
    fprintf(fp, "Compact wallclock seconds : %.2f\n", wtperf->compact_wallclock_us / 1.0e6);
    fprintf(fp, "Compact completed : %d\n", wtperf->compact_done ? 1 : 0);
    fprintf(fp, "Compact return code : %d\n", wtperf->compact_ret);
    fprintf(fp, "Compact pages reviewed : %" PRIu64 "\n", wtperf->compact_pages_reviewed);
    fprintf(fp, "Compact pages rewritten : %" PRIu64 "\n", wtperf->compact_pages_rewritten);
    fprintf(fp, "Compact pages skipped : %" PRIu64 "\n", wtperf->compact_pages_skipped);
    fprintf(fp, "\n");
    fprintf(fp, "File size before compact bytes : %" PRIu64 "\n", wtperf->compact_pre_file_size);
    fprintf(fp, "File size after compact bytes : %" PRIu64 "\n", wtperf->compact_post_file_size);
    fprintf(fp, "File size reduction bytes : %" PRId64 "\n",
      (int64_t)wtperf->compact_pre_file_size - (int64_t)wtperf->compact_post_file_size);
    fprintf(fp, "File size reduction pct : %.2f\n", reduction_pct);
    fprintf(fp, "\n");
    fprintf(fp, "Block reuse bytes before compact : %" PRIu64 "\n",
      wtperf->compact_pre_reuse_bytes);
    fprintf(fp, "Block reuse bytes after compact : %" PRIu64 "\n",
      wtperf->compact_post_reuse_bytes);
    fprintf(fp, "Block first srch walk time peak usecs : %" PRIu64 "\n",
      wtperf->block_first_srch_walk_peak_us);
    fprintf(fp, "\n");
    fprintf(fp, "Post-populate remove records : %" PRIu64 "\n",
      wtperf->post_populate_remove_records);
    fprintf(fp, "Post-populate remove seconds : %.2f\n",
      wtperf->post_populate_remove_us / 1.0e6);
    fprintf(fp, "\n");
    fprintf(fp, "Update ops during compact : %" PRIu64 "\n", ops_during);
    fprintf(fp, "Update avg latency during compact us : %.0f\n", avg_lat_us);
    fprintf(fp, "Update max latency during compact us : %" PRIu64 "\n",
      wtperf->update_max_latency_during_compact_us);

    (void)fclose(fp);
}
```

- [ ] **Step 4: Add a forward declaration**

Near the other static decls:

```c
static void compact_stats_dump(WTPERF *);
```

- [ ] **Step 5: Wire it into the existing end-of-run reporting**

Find where `latency_print(wtperf)` is called near the end of `start_all_runs` (search for `latency_print`). After the existing `latency_print(wtperf);` call, add:

```c
    compact_stats_dump(wtperf);
```

- [ ] **Step 6: Rebuild and verify the summary file**

```bash
cmake --build build --target wtperf -j$(nproc)
./build/bench/wtperf/wtperf -O bench/wtperf/runners/small-btree.wtperf \
    -o compact_threads=1 -o post_populate_remove_modulus=3 \
    -o "threads=((count=1,updates=1))" -o run_time=30 \
    -o icount=20000

cat build/bench/wtperf/WT_TEST/compact_summary.txt
```

Expected: the file exists and contains all the keys defined in the spec (Compact wallclock seconds, pages rewritten, file size before/after, etc.).

Tidy up:

```bash
rm -rf build/bench/wtperf/WT_TEST
```

- [ ] **Step 7: Commit**

```bash
git add bench/wtperf/wtperf.c
git commit -m "WT-14196 wtperf: write compact_summary.txt with benchmark metrics"
```

---

## Task 7: Add canonical and smoke runner configs

**Files:**
- Create: `bench/wtperf/runners/compact-stress.wtperf`
- Create: `bench/wtperf/runners/compact-stress-smoke.wtperf`

- [ ] **Step 1: Create `compact-stress.wtperf`**

Write `bench/wtperf/runners/compact-stress.wtperf`:

```ini
# WT-14196 compact-under-load benchmark.
# Reproduces the ticket scenario: populate a row-store with varied values,
# remove every 3rd key to fragment, then run a foreground compact in parallel
# with random updates and periodic checkpoints.

conn_config="cache_size=20G,statistics=(all),statistics_log=(json,on_close,wait=1)"
table_config="allocation_size=4KB,leaf_page_max=32KB,leaf_value_max=64MB,memory_page_max=10M,split_pct=90,key_format=Q,value_format=u"

# Populate: 1M records, varied 512B-4KB values, ~5GB on disk.
icount=1000000
populate_threads=1
value_sz_min=512
value_sz_max=4096
value_sz=2048
random_value=true

# Fragmentation: remove every 3rd key after populate.
post_populate_remove_modulus=3

# Workload phase: one update thread, one checkpoint thread, one compact thread.
threads=((count=1,updates=1))
run_time=1800
checkpoint_threads=1
checkpoint_interval=15
compact_threads=1
compact_start_after=0
compact_ends_workload=1

# Time-series monitor (existing) + per-op latency histogram (existing).
sample_interval=2
sample_rate=1
report_interval=10

# SLA gates — disabled until we have baseline runs to tune them.
min_throughput=0
max_latency=0
```

- [ ] **Step 2: Create `compact-stress-smoke.wtperf`**

Write `bench/wtperf/runners/compact-stress-smoke.wtperf` — a tiny variant that completes in <30 s for CI verification:

```ini
# WT-14196 smoke test for the compact-under-load benchmark.
# Completes in <30 seconds so the CI step can assert the compact_summary.txt
# format is intact.

conn_config="cache_size=512M,statistics=(all),statistics_log=(json,on_close,wait=1)"
table_config="allocation_size=4KB,leaf_page_max=32KB,memory_page_max=10M,split_pct=90,key_format=Q,value_format=u"

icount=20000
populate_threads=1
value_sz_min=512
value_sz_max=4096
value_sz=2048
random_value=true

post_populate_remove_modulus=3

threads=((count=1,updates=1))
run_time=60
checkpoint_threads=1
checkpoint_interval=5
compact_threads=1
compact_start_after=0
compact_ends_workload=1

sample_interval=2
sample_rate=1
report_interval=5

min_throughput=0
max_latency=0
```

- [ ] **Step 3: Run the smoke runner end-to-end**

```bash
./build/bench/wtperf/wtperf -O bench/wtperf/runners/compact-stress-smoke.wtperf
```

Expected: completes in <60 s, exits cleanly. `build/bench/wtperf/WT_TEST/compact_summary.txt` exists and contains:
- `Compact wallclock seconds : <some non-zero value>`
- `Compact pages rewritten : <non-zero>`
- `File size before compact bytes : <non-zero>`
- `File size after compact bytes : <non-zero>`
- `Post-populate remove records : ~6666` (20000 / 3)
- `Update ops during compact : <non-zero>`

Verify with:

```bash
grep -E 'Compact wallclock|pages rewritten|File size (before|after)|remove records|ops during compact' \
    build/bench/wtperf/WT_TEST/compact_summary.txt
```

Each line must appear with a non-empty value. Tidy up:

```bash
rm -rf build/bench/wtperf/WT_TEST
```

- [ ] **Step 4: Commit**

```bash
git add bench/wtperf/runners/compact-stress.wtperf bench/wtperf/runners/compact-stress-smoke.wtperf
git commit -m "WT-14196 wtperf: add compact-stress canonical and smoke runner configs"
```

---

## Task 8: `PerfStatLatencyPercentile` class

**Files:**
- Modify: `bench/perf_run_py/perf_stat.py` (add new class after existing `PerfStatLatency`)
- Create: `bench/perf_run_py/test_perf_stat_percentile.py` (small inline test)

The class reads `latency.update` CSV and extracts a percentile from the cumulative-operations column.

- [ ] **Step 1: Write the failing test**

Create `bench/perf_run_py/test_perf_stat_percentile.py`:

```python
#!/usr/bin/env python3
"""Lightweight test for PerfStatLatencyPercentile. Run with: python3 test_perf_stat_percentile.py"""
import os
import tempfile
import sys

# Allow import from the same directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from perf_stat import PerfStatLatencyPercentile

# A fixture latency.update CSV mirroring wtperf's format.
# 1000 ops total: 500 at <=1us, 400 at <=2us, 50 at <=10us, 40 at <=100us, 10 at <=1000us.
FIXTURE = (
    "#usecs,operations,cumulative-operations,total-operations\n"
    "1,500,500,1000\n"
    "2,400,900,1000\n"
    "10,50,950,1000\n"
    "100,40,990,1000\n"
    "1000,10,1000,1000\n"
)

def write_fixture(tmpdir, filename, content):
    p = os.path.join(tmpdir, filename)
    with open(p, 'w') as f:
        f.write(content)
    return p

def main():
    with tempfile.TemporaryDirectory() as d:
        write_fixture(d, "latency.update", FIXTURE)
        cases = [
            (50.0, 1),     # median is in the 1us bucket
            (95.0, 10),    # p95 falls in the 10us bucket
            (99.0, 100),   # p99 falls in the 100us bucket
            (99.9, 1000),  # p99.9 falls in the 1000us bucket
        ]
        for pct, expected in cases:
            stat = PerfStatLatencyPercentile(
                short_label=f"p{pct}",
                stat_file="latency.update",
                output_label=f"Update p{pct} latency us",
                percentile=pct,
                op_name="update",
            )
            values = stat.find_stat(os.path.join(d, "latency.update"))
            assert values, f"no value extracted for p{pct}"
            assert values[0] == expected, \
                f"p{pct}: expected {expected}, got {values[0]}"
            print(f"  p{pct}: {values[0]} us OK")
    print("PASS")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run test, expect failure**

```bash
cd /home/ubuntu/wiredtiger-sean-compact-perf
python3 bench/perf_run_py/test_perf_stat_percentile.py
```

Expected: `ImportError: cannot import name 'PerfStatLatencyPercentile' from 'perf_stat'`.

- [ ] **Step 3: Implement the class**

Open `bench/perf_run_py/perf_stat.py` and after the existing `PerfStatLatencyWorkgen` class (around line 171), add:

```python
class PerfStatLatencyPercentile(PerfStat):
    """Computes a percentile latency by reading a wtperf latency.<op> CSV.

    The CSV columns are: usecs, operations, cumulative-operations, total-operations.
    Each row is a histogram bucket: 'operations' ops with latency <= 'usecs'.
    We find the smallest bucket whose cumulative >= percentile * total / 100.
    """

    def __init__(self, short_label: str, stat_file: str, output_label: str,
                 percentile: float, op_name: str):
        super().__init__(short_label=short_label,
                         stat_file=stat_file,
                         output_label=output_label)
        self.percentile = percentile
        self.op_name = op_name

    def find_stat(self, test_stat_path: str):
        """Return [bucket_us] for the percentile, or [0] if the file is missing/empty."""
        if not os.path.exists(test_stat_path):
            return [0]
        last_us = 0
        total = 0
        with open(test_stat_path) as f:
            for line in f:
                if line.startswith('#') or not line.strip():
                    continue
                parts = line.strip().split(',')
                if len(parts) < 4:
                    continue
                try:
                    us = int(parts[0])
                    cum = int(parts[2])
                    total = int(parts[3])
                except ValueError:
                    continue
                last_us = us
                # First bucket whose cumulative crosses the threshold wins.
                if total > 0 and cum * 100 >= self.percentile * total:
                    return [us]
        # Below threshold for entire file: return the largest bucket we saw.
        return [last_us]
```

- [ ] **Step 4: Run test, expect pass**

```bash
python3 bench/perf_run_py/test_perf_stat_percentile.py
```

Expected output:

```
  p50.0: 1 us OK
  p95.0: 10 us OK
  p99.0: 100 us OK
  p99.9: 1000 us OK
PASS
```

- [ ] **Step 5: Commit**

```bash
git add bench/perf_run_py/perf_stat.py bench/perf_run_py/test_perf_stat_percentile.py
git commit -m "WT-14196 perf_run_py: add PerfStatLatencyPercentile class"
```

---

## Task 9: `compact_stats()` PerfStat list and wiring into `all_stats()`

**Files:**
- Modify: `bench/perf_run_py/perf_stat_collection.py` (new method + extend `all_stats()`)

- [ ] **Step 1: Add the new method**

Open `bench/perf_run_py/perf_stat_collection.py`. After the `cache_eviction_stats` classmethod (which ends around line 117), add:

```python
    @staticmethod
    def compact_stats():
        """Stats produced by wtperf compact-stress runner: compact_summary.txt + latency.update."""
        return [
            PerfStat(short_label="compact_wallclock_sec",
                     stat_file='compact_summary.txt',
                     pattern=r'Compact wallclock seconds\s+:\s+[\d.]+',
                     input_offset=4, output_precision=2,
                     conversion_function=float,
                     output_label='Compact wallclock seconds'),
            PerfStat(short_label="compact_pages_reviewed",
                     stat_file='compact_summary.txt',
                     pattern=r'Compact pages reviewed\s+:\s+\d+',
                     input_offset=4,
                     output_label='Compact pages reviewed'),
            PerfStat(short_label="compact_pages_rewritten",
                     stat_file='compact_summary.txt',
                     pattern=r'Compact pages rewritten\s+:\s+\d+',
                     input_offset=4,
                     output_label='Compact pages rewritten'),
            PerfStat(short_label="compact_pages_skipped",
                     stat_file='compact_summary.txt',
                     pattern=r'Compact pages skipped\s+:\s+\d+',
                     input_offset=4,
                     output_label='Compact pages skipped'),
            PerfStat(short_label="file_size_reduction_bytes",
                     stat_file='compact_summary.txt',
                     pattern=r'File size reduction bytes\s+:\s+-?\d+',
                     input_offset=5,
                     output_label='File size reduction bytes'),
            PerfStat(short_label="block_reuse_bytes_after",
                     stat_file='compact_summary.txt',
                     pattern=r'Block reuse bytes after compact\s+:\s+\d+',
                     input_offset=6,
                     output_label='Block reuse bytes after compact'),
            PerfStat(short_label="block_first_srch_walk_peak_us",
                     stat_file='compact_summary.txt',
                     pattern=r'Block first srch walk time peak usecs\s+:\s+\d+',
                     input_offset=8,
                     output_label='Block first srch walk time peak usecs'),
            PerfStat(short_label="update_ops_during_compact",
                     stat_file='compact_summary.txt',
                     pattern=r'Update ops during compact\s+:\s+\d+',
                     input_offset=5,
                     output_label='Update ops during compact'),
            PerfStat(short_label="update_avg_latency_during_compact_us",
                     stat_file='compact_summary.txt',
                     pattern=r'Update avg latency during compact us\s+:\s+\d+',
                     input_offset=7,
                     output_label='Update avg latency during compact us'),
            PerfStat(short_label="update_max_latency_during_compact_us",
                     stat_file='compact_summary.txt',
                     pattern=r'Update max latency during compact us\s+:\s+\d+',
                     input_offset=7,
                     output_label='Update max latency during compact us'),
            PerfStatLatencyPercentile(short_label="update_p50_latency_us",
                                      stat_file='latency.update',
                                      output_label='Update p50 latency us',
                                      percentile=50.0, op_name='update'),
            PerfStatLatencyPercentile(short_label="update_p95_latency_us",
                                      stat_file='latency.update',
                                      output_label='Update p95 latency us',
                                      percentile=95.0, op_name='update'),
            PerfStatLatencyPercentile(short_label="update_p99_latency_us",
                                      stat_file='latency.update',
                                      output_label='Update p99 latency us',
                                      percentile=99.0, op_name='update'),
            PerfStatLatencyPercentile(short_label="update_p999_latency_us",
                                      stat_file='latency.update',
                                      output_label='Update p99.9 latency us',
                                      percentile=99.9, op_name='update'),
        ]
```

Note: `input_offset` is the zero-based word index of the value after the regex matches, splitting on whitespace. Sample counting for `"Compact wallclock seconds : 145.32"`: `['Compact'(0), 'wallclock'(1), 'seconds'(2), ':'(3), '145.32'(4)]` → 4. For `"Block first srch walk time peak usecs : 23145"`: positions 0..8 → 8.

- [ ] **Step 2: Add the import**

At the top of `perf_stat_collection.py`, the existing import line is:

```python
from perf_stat import PerfStat, PerfStatCount, PerfStatLatency, PerfStatMinMax, PerfStatLatencyWorkgen, PerfStatDBSize
```

Extend it to include `PerfStatLatencyPercentile`:

```python
from perf_stat import PerfStat, PerfStatCount, PerfStatLatency, PerfStatMinMax, PerfStatLatencyWorkgen, PerfStatDBSize, PerfStatLatencyPercentile
```

- [ ] **Step 3: Include in `all_stats()`**

`all_stats` ends with `] + PerfStatCollection.cache_eviction_stats()` (around line 246). Change to:

```python
        ] + PerfStatCollection.cache_eviction_stats() + PerfStatCollection.compact_stats()
```

- [ ] **Step 4: Smoke test perf_run.py against the smoke runner**

Rebuild wtperf first if not already up to date:

```bash
cmake --build build --target wtperf -j$(nproc)
```

Then run perf_run.py against the smoke runner:

```bash
cd build/bench/wtperf
rm -rf WT_TEST*
python3 ../../../bench/perf_run_py/perf_run.py --wtperf \
    -e ./wtperf -t ../../../bench/wtperf/runners/compact-stress-smoke.wtperf \
    -ho WT_TEST -m 1 -v -b \
    -o test_stats/evergreen_out_smoke.json \
    -ops '["compact_wallclock_sec","compact_pages_rewritten","file_size_reduction_bytes","block_first_srch_walk_peak_us","update_ops_during_compact","update_p50_latency_us","update_p99_latency_us"]'
cat test_stats/evergreen_out_smoke.json
cd /home/ubuntu/wiredtiger-sean-compact-perf
```

Expected: the JSON output contains entries for each requested short_label with values >= 0. None should be `null` or missing.

- [ ] **Step 5: Commit**

```bash
git add bench/perf_run_py/perf_stat_collection.py
git commit -m "WT-14196 perf_run_py: report compact_summary.txt + latency.update stats"
```

---

## Task 10: Evergreen task definitions

**Files:**
- Modify: `test/evergreen.yml` (add two new tasks near the existing `wtperf-test-*` block around line 5718)

The exact line range will drift; we anchor on adjacent existing task names.

- [ ] **Step 1: Locate the existing wtperf-test block**

```bash
grep -n 'name: wtperf-test-' test/evergreen.yml | head
```

Note the first such task name (often `wtperf-test-small-btree`). We will add the new tasks immediately before that block so they sort first under the `wtperf-perf-test` tag.

- [ ] **Step 2: Add the canonical task**

Before the existing first `wtperf-test-*` task, insert:

```yaml
  - name: wtperf-test-compact-stress
    tags: ["wtperf-perf-test"]
    depends_on:
      - name: compile
    commands:
      - func: "fetch artifacts"
      - func: "run-perf-test"
        vars:
          perf-test-name: compact-stress.wtperf
          maxruns: 3
          wtarg: -ops ['"compact_wallclock_sec", "compact_pages_reviewed",
                        "compact_pages_rewritten", "compact_pages_skipped",
                        "file_size_reduction_bytes", "block_reuse_bytes_after",
                        "block_first_srch_walk_peak_us", "update_ops_during_compact",
                        "update_max_latency_during_compact_us",
                        "update_avg_latency_during_compact_us",
                        "update_p50_latency_us", "update_p95_latency_us",
                        "update_p99_latency_us", "update_p999_latency_us",
                        "min_max_update_throughput", "max_latency_read_update"']
      - func: "convert-to-atlas-evergreen-format"
        vars:
          input_file:  ./wiredtiger/cmake_build/bench/wtperf/test_stats/atlas_out_compact-stress.wtperf.json
          output_path: ./wiredtiger/cmake_build/bench/wtperf/test_stats/atlas_out_compact-stress.json
          test_name:   compact-stress
      - func: "upload atlas perf test results"
        vars:
          test-name: compact-stress
      - func: "upload test stats"
        vars:
          test_path: bench/wtperf/test_stats/evergreen_out_compact-stress.wtperf
```

- [ ] **Step 3: Add the medium variant**

Immediately after the canonical task:

```yaml
  - name: wtperf-test-compact-stress-medium
    tags: ["wtperf-perf-test"]
    depends_on:
      - name: compile
    commands:
      - func: "fetch artifacts"
      - func: "run-perf-test"
        vars:
          perf-test-name: compact-stress.wtperf
          maxruns: 2
          wtarg: -args ['"-o icount=5000000", "-o run_time=3600",
                         "-o conn_config=cache_size=10G"']
                 -ops ['"compact_wallclock_sec", "compact_pages_reviewed",
                        "compact_pages_rewritten", "compact_pages_skipped",
                        "file_size_reduction_bytes", "block_reuse_bytes_after",
                        "block_first_srch_walk_peak_us", "update_ops_during_compact",
                        "update_max_latency_during_compact_us",
                        "update_avg_latency_during_compact_us",
                        "update_p50_latency_us", "update_p95_latency_us",
                        "update_p99_latency_us", "update_p999_latency_us",
                        "min_max_update_throughput", "max_latency_read_update"']
      - func: "convert-to-atlas-evergreen-format"
        vars:
          input_file:  ./wiredtiger/cmake_build/bench/wtperf/test_stats/atlas_out_compact-stress.wtperf.json
          output_path: ./wiredtiger/cmake_build/bench/wtperf/test_stats/atlas_out_compact-stress-medium.json
          test_name:   compact-stress-medium
      - func: "upload atlas perf test results"
        vars:
          test-name: compact-stress-medium
      - func: "upload test stats"
        vars:
          test_path: bench/wtperf/test_stats/evergreen_out_compact-stress.wtperf
```

- [ ] **Step 4: YAML lint**

```bash
python3 -c "import yaml; yaml.safe_load(open('test/evergreen.yml'))"
```

Expected: no output (clean parse). A `ScannerError` or `ParserError` means an indentation mistake — fix before continuing.

- [ ] **Step 5: Verify the tasks are picked up by the existing build-variants**

The `wtperf-perf-test` tag is already referenced by Evergreen build-variants in the same file. Verify both new tasks would be selected by the tag:

```bash
grep -A2 'name: wtperf-test-compact-stress' test/evergreen.yml | head -10
```

Expected: each block shows `tags: ["wtperf-perf-test"]`.

- [ ] **Step 6: Commit**

```bash
git add test/evergreen.yml
git commit -m "WT-14196 evergreen: add compact-stress and compact-stress-medium perf tasks"
```

---

## Task 11: Baseline validation and differential check

This task produces no code changes, but documents that the benchmark distinguishes baseline `develop` from a prototype solution. It is the gate for declaring v1 done.

**Files:**
- Create: `dev-notes/specs/2026-05-11-wt14196-baseline-results.md` (results of the validation runs)

- [ ] **Step 1: Run smoke on develop (no compact threads)**

A control run, with compact disabled, to verify the benchmark itself doesn't perturb the workload:

```bash
cd /home/ubuntu/wiredtiger-sean-compact-perf
cmake --build build --target wtperf -j$(nproc)
rm -rf build/bench/wtperf/WT_TEST*
./build/bench/wtperf/wtperf -O bench/wtperf/runners/compact-stress-smoke.wtperf -o compact_threads=0
cp build/bench/wtperf/WT_TEST/compact_summary.txt /tmp/control_summary.txt || true
cp build/bench/wtperf/WT_TEST/monitor /tmp/control_monitor || true
cp build/bench/wtperf/WT_TEST/latency.update /tmp/control_latency.update
```

With `compact_threads=0`, `compact_stats_dump` returns early, so the control run won't produce `compact_summary.txt` — that's expected. The monitor + latency.update give us the no-compact-baseline throughput/latency.

- [ ] **Step 2: Run smoke with compact enabled**

```bash
rm -rf build/bench/wtperf/WT_TEST*
./build/bench/wtperf/wtperf -O bench/wtperf/runners/compact-stress-smoke.wtperf
cp build/bench/wtperf/WT_TEST/compact_summary.txt /tmp/treatment_summary.txt
cp build/bench/wtperf/WT_TEST/monitor /tmp/treatment_monitor
cp build/bench/wtperf/WT_TEST/latency.update /tmp/treatment_latency.update
```

- [ ] **Step 3: Verify qualitative behaviours**

Expected from `/tmp/treatment_summary.txt`:
- `Compact pages rewritten : <non-zero>` — compact did real work
- `File size reduction bytes : <positive>` — file shrank
- `Block first srch walk time peak usecs : <non-zero>` — the pathology fired

Compare update throughput between `/tmp/control_monitor` and `/tmp/treatment_monitor`: the average ops/sec in the treatment run should be measurably lower than in the control run. (Eyeball the columns; precise comparison comes later via perf_run.py.)

- [ ] **Step 4: Document baseline numbers**

Create `dev-notes/specs/2026-05-11-wt14196-baseline-results.md`:

```markdown
# WT-14196 Baseline Results

These results gate v1 of the compact-stress benchmark — they confirm the
benchmark exhibits the WT-14196 pathology on unmodified `develop` and that
update throughput drops measurably during compact compared to a
`compact_threads=0` control.

## Setup

- Branch: develop @ <git rev-parse HEAD>
- Build: Release, ENABLE_PYTHON=0, HAVE_DIAGNOSTIC=0
- Runner: bench/wtperf/runners/compact-stress-smoke.wtperf

## Control (compact_threads=0)

| Metric | Value |
|---|---|
| Avg update throughput (ops/sec) | <fill in from /tmp/control_monitor> |
| Run wallclock seconds | <fill in> |

## Treatment (compact_threads=1)

| Metric | Value |
|---|---|
| Compact wallclock seconds | <fill in from /tmp/treatment_summary.txt> |
| Compact pages rewritten | <fill in> |
| File size reduction bytes | <fill in> |
| Block first srch walk time peak usecs | <fill in> |
| Update ops during compact | <fill in> |
| Update avg latency during compact us | <fill in> |
| Update max latency during compact us | <fill in> |
| Avg update throughput (ops/sec) | <fill in from /tmp/treatment_monitor> |

## Conclusion

- [ ] Compact did real work (pages_rewritten > 0, file_size_reduction > 0)
- [ ] block_first_srch_walk_peak_us is non-trivial (>> 0)
- [ ] Treatment avg update throughput < control avg update throughput by at least 10%

If any of the above are unchecked, the benchmark is not measuring the right
thing. Iterate before moving on.

## Differential check (prototype branch)

Cherry-picking `origin/wt-14196-compact-block-first-srch-v2-prototype`
into a separate worktree, the same smoke runner produces:

| Metric | Baseline | Prototype | Delta |
|---|---|---|---|
| Compact wallclock seconds | ... | ... | ... |
| Block first srch walk time peak usecs | ... | ... | ... |
| Update ops during compact | ... | ... | ... |

The prototype should reduce `block_first_srch_walk_peak_us` substantially
and improve update ops during compact. If it doesn't, the benchmark needs
adjustment.
```

Fill in the actual numbers from the runs above.

- [ ] **Step 5: Differential check against prototype branch (optional but recommended)**

In a separate worktree (so the current branch state isn't perturbed):

```bash
cd /home/ubuntu
git clone --reference /home/ubuntu/wiredtiger /home/ubuntu/wiredtiger /home/ubuntu/wt-proto || true
cd /home/ubuntu/wt-proto
git fetch origin wt-14196-compact-block-first-srch-v2-prototype
git checkout origin/wt-14196-compact-block-first-srch-v2-prototype
# Cherry-pick the wtperf changes onto the prototype.
git cherry-pick <commit hashes from sean-compact-perf>
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DENABLE_PYTHON=0 -DHAVE_DIAGNOSTIC=0
cmake --build build --target wtperf -j$(nproc)
rm -rf build/bench/wtperf/WT_TEST*
./build/bench/wtperf/wtperf -O bench/wtperf/runners/compact-stress-smoke.wtperf
cp build/bench/wtperf/WT_TEST/compact_summary.txt /tmp/prototype_summary.txt
```

Compare `/tmp/treatment_summary.txt` (baseline) to `/tmp/prototype_summary.txt` (with prototype) — the prototype should show measurably lower `block_first_srch_walk_peak_us` and higher `update_ops_during_compact`.

If the benchmark cannot distinguish the two, return to Task 5/6 and re-examine the sampler frequency, the snapshot timing, or whether `compact_start_after=0` is the right configuration.

- [ ] **Step 6: Commit the baseline notes**

```bash
cd /home/ubuntu/wiredtiger-sean-compact-perf
git add dev-notes/specs/2026-05-11-wt14196-baseline-results.md
git commit -m "WT-14196 dev-notes: baseline and differential results for compact perf benchmark"
```

---

## Final verification before raising a PR

- [ ] All tasks committed; `git log develop..sean-compact-perf` shows ~10 commits
- [ ] `cmake --build build --target wtperf -j$(nproc)` clean
- [ ] `python3 bench/perf_run_py/test_perf_stat_percentile.py` prints `PASS`
- [ ] `python3 -c "import yaml; yaml.safe_load(open('test/evergreen.yml'))"` no output
- [ ] `./build/bench/wtperf/wtperf -O bench/wtperf/runners/compact-stress-smoke.wtperf` completes <60s and writes `compact_summary.txt`
- [ ] `dist/s_all` from the repo root passes (clang-format + s_fast checks)
- [ ] Baseline results filled in at `dev-notes/specs/2026-05-11-wt14196-baseline-results.md`

Submit a draft PR titled `WT-14196 wtperf: foreground compact-under-load benchmark` for wtperf-team review.
