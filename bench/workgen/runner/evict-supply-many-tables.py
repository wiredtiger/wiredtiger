#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

# Eviction candidate-supply workload with many tables.
#
# Thousands of tables each hold a small tree, and only a quarter of them are ever written. Updates
# are spread over those tables, so every written tree has only a few dirty pages and the single
# eviction server has to walk a lot of trees to find a little to evict. Reader threads over a few
# large read-only tables keep the cache full of clean pages. Timestamped transactions with a lagging
# oldest timestamp keep older versions, so evicted pages write to the history store.
#
# The run is supply-bound when the dirty bytes sit above the dirty trigger, the dirty-hard eviction
# state is set most of the time, most application threads wait for cache, and the eviction server
# candidate queue is empty at most refills. The throughput is then set by how many candidates each
# eviction walk queues, which makes the workload a baseline for changes to the eviction walk.
#
# Everything runs in one process: populate, read table populate, prewarm, then the timed run. The
# defaults are the full shape (about 47 GB on disk; use a host with enough memory to hold it in the
# page cache, otherwise the run is disk-bound instead of supply-bound). Use smaller values for a
# smoke run:
#   python3 evict-supply-many-tables.py --home WT_TEST --tables 500 --duration 60
# Use --report HOME to print the metrics of an earlier run again.
#
# The metrics come from the statistics log of the timed run. Each is the change over the reporting
# window, which starts --window-start seconds after the first update.
import argparse
import glob
import json
import os
import resource
import sys

from runner import *
from wiredtiger import *
from workgen import *

KEY_SIZE = 20
VALUE_SIZE = 500
TXN_OPS = 20
OLDEST_LAG = 30
STABLE_LAG = 10
READERS = 32
WRITERS = 21
EVICT_THREADS = 12
POPULATE_THREADS = 8
READ_TABLES = 4
TABLE_CONFIG = (
    "key_format=S,value_format=S,allocation_size=4kb,internal_page_max=4kb,"
    "leaf_page_max=4kb,split_pct=100,type=file,log=(enabled=false)")
READ_TABLE_CONFIG = (
    "key_format=S,value_format=S,allocation_size=4kb,internal_page_max=16kb,"
    "leaf_page_max=32kb,split_pct=100,type=file,log=(enabled=false)")
# Open handles needed beyond one per table: the history store, the metadata, and slack.
EXTRA_FILES = 1000


def make_parser():
    p = argparse.ArgumentParser("Eviction candidate supply with many tables.")
    p.add_argument("--tables", type=int, default=10000, help="number of tables")
    p.add_argument("--rows", type=int, default=8000, help="rows per table")
    p.add_argument("--write-tables", type=int, default=None,
                   help="number of tables written (default: a quarter of the tables)")
    p.add_argument("--read-rows", type=int, default=1200000, help="rows per read table")
    p.add_argument("--cache", default="4GB", help="cache size")
    p.add_argument("--duration", type=int, default=300, help="timed run length in seconds")
    p.add_argument("--conn-extra", default="",
                   help="extra connection configuration for the timed run")
    p.add_argument("--no-prewarm", action="store_true", help="skip the scan that fills the cache")
    p.add_argument("--window-start", type=int, default=120,
                   help="seconds after the first update at which the metrics window starts")
    p.add_argument("--report", metavar="HOME", help="print the metrics of an earlier run and exit")
    return p


def raise_nofile(needed):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= needed:
        return
    if hard != resource.RLIM_INFINITY and hard < needed:
        sys.exit("open file limit is %d, at least %d is needed; raise the hard limit "
                 "(for example prlimit --nofile=%d)" % (hard, needed, needed))
    resource.setrlimit(resource.RLIMIT_NOFILE, (needed, hard))


def make_tables(count, rows, prefix="test"):
    """Keys are zero-padded random numbers below rows."""
    tables = []
    for i in range(count):
        t = Table("table:" + prefix + str(i))
        t.options.key_size = KEY_SIZE
        t.options.value_size = VALUE_SIZE
        t.options.range = rows
        tables.append(t)
    return tables


def populate(context, count, rows, prefix, table_config):
    conn = context.wiredtiger_open(
        "create,cache_size=8GB,eviction=(threads_max=8),session_max=1000,"
        "file_manager=(close_idle_time=0)")
    s = conn.open_session("")
    for i in range(count):
        s.create("table:" + prefix + str(i), table_config)
    tables = make_tables(count, 0, prefix)
    ops = op_multi_table(Operation(Operation.OP_INSERT, tables[0]), tables)
    pop = Workload(context, POPULATE_THREADS * Thread(ops * (rows // POPULATE_THREADS)))
    ret = pop.run(conn)
    assert ret == 0, ret
    s.checkpoint()
    conn.close()
    print("populate complete: %d %s tables x %d rows" % (count, prefix, rows), flush=True)


def prewarm(conn, count):
    s = conn.open_session("")
    for i in range(count):
        c = s.open_cursor("table:test" + str(i))
        while c.next() == 0:
            pass
        c.close()
    s.close()
    print("prewarm complete", flush=True)


def reopen_op(optype, table):
    # A cursor reopened for every operation goes back to the cursor cache, rather than each thread
    # holding an open cursor on every table.
    op = Operation(optype, table)
    op._config = "reopen"
    return op


def writer_threads(write_tbls):
    per_writer = len(write_tbls) // WRITERS
    writers = []
    for w in range(WRITERS):
        mine = write_tbls[w * per_writer:(w + 1) * per_writer]
        txns = []
        for i in range(0, len(mine), TXN_OPS):
            group = Operation()
            group._group = OpList([reopen_op(Operation.OP_UPDATE, t) for t in mine[i:i + TXN_OPS]])
            group._repeatgroup = 1
            txn(group, "isolation=snapshot")
            group.transaction.use_commit_timestamp = True
            txns.append(group)
        top = Operation()
        top._group = OpList(txns)
        top._repeatgroup = 1
        t = Thread(top)
        t.options.name = "update"
        writers.append(t)
    return ThreadListWrapper(ThreadList(writers))


def run(context, args, write_tables):
    conn = context.wiredtiger_open(
        "create,cache_size=%s,eviction=(threads_min=%d,threads_max=%d),session_max=1000,"
        "file_manager=(close_idle_time=100000),"
        "statistics=(fast),statistics_log=(wait=1,json=true,on_close=true),"
        "eviction_updates_target=1,eviction_updates_trigger=40,checkpoint=(wait=5)%s" %
        (args.cache, EVICT_THREADS, EVICT_THREADS,
         "," + args.conn_extra if args.conn_extra else ""))
    if not args.no_prewarm:
        prewarm(conn, args.tables)

    write_tbls = make_tables(write_tables, args.rows)
    read_tbls = make_tables(READ_TABLES, args.read_rows, "read")
    threads = writer_threads(write_tbls)
    reader = Thread(op_multi_table(reopen_op(Operation.OP_SEARCH, read_tbls[0]), read_tbls))
    reader.options.name = "read"
    threads = threads + READERS * reader

    wl = Workload(context, threads)
    wl.options.run_time = args.duration
    wl.options.report_interval = 10
    wl.options.max_latency = 1000
    wl.options.sample_rate = 1
    wl.options.oldest_timestamp_lag = OLDEST_LAG
    wl.options.stable_timestamp_lag = STABLE_LAG
    wl.options.timestamp_advance = 1
    ret = wl.run(conn)
    assert ret == 0, ret
    conn.close()


def load_stats(home, window_start):
    """Statistics log samples from window_start seconds after the first update to the last one."""
    rows = []
    for path in sorted(glob.glob(home + "/WiredTigerStat.*")):
        with open(path, errors="replace") as f:
            for line in f:
                try:
                    doc = json.loads(line)
                except ValueError:
                    continue
                flat = {}
                for vals in doc.get("wiredTiger", {}).values():
                    if isinstance(vals, dict):
                        flat.update(vals)
                rows.append(flat)
    upd = [r.get("cursor update calls", 0) for r in rows]
    moving = [i for i in range(1, len(rows)) if upd[i] > upd[i - 1]]
    if len(moving) < 20:
        return None
    steady = rows[moving[0]:moving[-1] + 1]
    # A run shorter than the window start still skips the ramp-up.
    start = window_start if len(steady) > window_start + 10 else 10
    return steady[start:]


def median(rows, key):
    vals = sorted(r.get(key, 0) for r in rows)
    return vals[len(vals) // 2]


def report(home, window_start):
    rows = load_stats(home, window_start)
    if rows is None or len(rows) < 2:
        print("no steady update window in %s" % home)
        return None
    first, last = rows[0], rows[-1]
    secs = len(rows) - 1

    def delta(key):
        return last.get(key, 0) - first.get(key, 0)

    visits = (delta("eviction walks started from root of tree") +
              delta("eviction walks started from saved location in tree")) or 1
    seen = delta("pages seen by eviction walk") or 1
    queued = delta("pages queued for eviction")
    refills = (delta("eviction server candidate queue empty when topping up") +
               delta("eviction server candidate queue not empty when topping up")) or 1
    max_bytes = median(rows, "maximum bytes configured") or 1
    states = [r.get("eviction state", 0) for r in rows]
    metrics = [
        ("Queue empty at refill (%)", 100 * delta(
            "eviction server candidate queue empty when topping up") / refills, 90),
        ("Eviction empty score (median)", median(rows, "eviction empty score"), 100),
        ("Refs walked per visit", delta("pages walked for eviction") / visits, 656),
        ("Pages seen per visit", seen / visits, 11.6),
        ("Pages queued per visit", queued / visits, 3.0),
        ("Visits giving up with nothing queued (%)", 100 * delta(
            "eviction walks gave up because they saw too many pages and found no candidates") /
            visits, 8.3),
        ("Dirty bytes (%)", 100 * median(rows, "tracked dirty bytes in the cache") / max_bytes,
         23.2),
        ("Cache fill (%)", 100 * median(rows, "bytes currently in the cache") / max_bytes, 86),
        ("Dirty hard set (% of samples)",
         100 * sum(1 for s in states if s & 0x8) / len(states), 93),
        ("Pages queued per second", queued / secs, None),
        ("Updates per second", delta("cursor update calls") / secs, None),
        ("Searches per second", delta("cursor search calls") / secs, None),
    ]
    print("Metrics over %d seconds of %s" % (secs, home))
    print("%-45s %12s %12s" % ("Metric", "Measured", "Reference"))
    for name, value, ref in metrics:
        print("%-45s %12.4g %12s" % (name, value, "" if ref is None else "%.4g" % ref))
    return metrics


def main():
    parser = make_parser()
    args, rest = parser.parse_known_args()
    if args.report:
        report(args.report, args.window_start)
        return
    write_tables = args.write_tables if args.write_tables is not None else args.tables // 4
    if write_tables < WRITERS:
        sys.exit("at least %d tables must be written, one for each writer thread" % WRITERS)
    raise_nofile(args.tables + READ_TABLES + EXTRA_FILES)

    # The workgen context parses the command line itself; hand it only what it knows.
    sys.argv = [sys.argv[0]] + rest
    context = Context()
    populate(context, args.tables, args.rows, "test", TABLE_CONFIG)
    populate(context, READ_TABLES, args.read_rows, "read", READ_TABLE_CONFIG)
    run(context, args, write_tables)
    report(context.args.home, args.window_start)


if __name__ == "__main__":
    main()
