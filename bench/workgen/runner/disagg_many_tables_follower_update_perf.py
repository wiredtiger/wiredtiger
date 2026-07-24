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
# disagg_many_tables_follower_update_perf.py
#   Disagg follower pick-up performance for the many-table *update* path.
#   Measures how long it takes a follower that already owns N layered tables
#   to pick up a new checkpoint after the leader has dirtied those tables'
#   shared file: checkpoint metadata (the __disagg_update_file_meta path
#   inside __disagg_apply_checkpoint_meta).
#
#     Phase 1 (leader):     create N layered tables, checkpoint (meta1).
#     Phase 2 (follower):   open, pick up meta1 (setup; establishes local
#                           metadata for all N tables). Unmeasured primary.
#     Phase 3 (leader):     one tiny timestamped insert per table, bump
#                           stable, checkpoint (meta2; dirties shared file:
#                           checkpoint cookies).
#     Phase 4 (follower):   reconfigure(checkpoint_meta=meta2) timed; report
#                           wall-clock pick-up plus
#                           disagg_apply_checkpoint_meta_time and
#                           disagg_pick_up_file_meta_updated.
#
#   Leader and follower use separate WT homes that share one PALI store:
#     <home>/                 leader
#       kv_home/
#     <home>/follower/        follower
#       kv_home -> ../kv_home
#
#   Env: WT_BUILDDIR must point at the build dir containing
#        ext/page_log/palite/libwiredtiger_palite.so.
#

from runner import *
from wiredtiger import *
from wiredtiger import stat
from workgen import *
import os, time

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
PAGE_LOG = "palite"
TABLE_PREFIX = "test_disagg_update_pickup_"
TABLE_CFG = "key_format=S,value_format=S,type=layered,block_manager=disagg"

context = Context()
context.parser.add_argument("--num-tables", dest="num_tables", type=int, default=10000,
    help="Number of layered tables (default: 10000)")
context.initialize()
home = context.args.home
NUM_TABLES = context.args.num_tables

follower_home = os.path.join(home, "follower")
os.mkdir(follower_home)
# Pre-create the shared PALI store and symlink it into the follower home
# (same layout as helper_disagg.early_setup).
os.mkdir(os.path.join(home, "kv_home"))
os.symlink("../kv_home", os.path.join(follower_home, "kv_home"), target_is_directory=True)

wt_builddir = os.environ.get("WT_BUILDDIR")
if not wt_builddir:
    raise RuntimeError("WT_BUILDDIR must be set (path to the build dir)")
ext_path = os.path.join(wt_builddir, "ext", "page_log", PAGE_LOG,
                        "libwiredtiger_" + PAGE_LOG + ".so")
if not os.path.isfile(ext_path):
    raise RuntimeError("page_log extension not found at " + ext_path)

base_conn_config = (
    f"statistics=(all),statistics_log=(wait=1,on_close,json=true),"
    f"cache_size=20GB,precise_checkpoint=true,"
    # Aggressive sweep: scan every 1s, expire dhandles after 2s of idleness,
    # don't keep a floor of 250 open. Keeps fd usage bounded under our
    # tight create / mutate loops.
    f"file_manager=(close_handle_minimum=10,close_idle_time=2,close_scan_interval=1),"
    f'extensions=("{ext_path}"=(config="(verbose=0)")),'
    f"disaggregated=(page_log={PAGE_LOG},lose_all_my_data=true,"
)


def fetch_checkpoint_meta(conn):
    print("  fetching checkpoint_meta from PALI")
    page_log = conn.get_page_log(PAGE_LOG)
    meta_session = conn.open_session()
    try:
        (_, _, _, ckpt_meta) = page_log.pl_get_complete_checkpoint(meta_session)
    except Exception as ex:
        if "WT_NOTFOUND" not in str(ex):
            raise
        kv = os.path.join(home, "kv_home", "checkpoints.db")
        raise RuntimeError(
            "pl_get_complete_checkpoint: WT_NOTFOUND (no completed checkpoint in PALI). "
            f"Expected PALI DB roughly at: {kv}"
        ) from ex
    finally:
        page_log.terminate(meta_session)
        meta_session.close()
    assert ckpt_meta, "no complete checkpoint metadata returned from PALI"
    print(f"  checkpoint_meta length: {len(ckpt_meta)} bytes")
    return ckpt_meta


def get_conn_stat(conn, stat_field):
    session = conn.open_session()
    try:
        c = session.open_cursor("statistics:", None, None)
        try:
            return c[stat_field][2]
        finally:
            c.close()
    finally:
        session.close()


# ----------------------------------------------------------------------
# Phase 1: leader creates N layered tables and checkpoints.
# ----------------------------------------------------------------------
print("=" * 70)
print(f"Phase 1: leader creating {NUM_TABLES} layered tables")
print("=" * 70)

leader_conn = wiredtiger_open(
    home, "create," + base_conn_config + 'role="leader")')
leader_session = leader_conn.open_session()

# Initialize timestamps before any writes so commits can use commit_timestamp.
leader_conn.set_timestamp("stable_timestamp=1")

t0 = time.time()
for i in range(NUM_TABLES):
    uri = f"table:{TABLE_PREFIX}{i}"
    leader_session.create(uri, TABLE_CFG)
    if (i + 1) % 10000 == 0:
        print(f"  created {i+1}/{NUM_TABLES}  ({time.time()-t0:.1f}s)")
        # Checkpoint allows dhandle memory to be released.
        leader_session.checkpoint()
print(f"  all {NUM_TABLES} tables created in {time.time()-t0:.1f}s")

print("  taking initial checkpoint (meta1)")
t0 = time.time()
leader_session.checkpoint()
print(f"  checkpoint completed in {time.time()-t0:.1f}s")

ckpt_meta1 = fetch_checkpoint_meta(leader_conn)

# ----------------------------------------------------------------------
# Phase 2: follower picks up meta1 (setup; establishes local metadata).
# ----------------------------------------------------------------------
print("=" * 70)
print("Phase 2: follower setup pick-up (meta1, unmeasured primary)")
print("=" * 70)

follower_conn = wiredtiger_open(
    follower_home, "create," + base_conn_config + 'role="follower")')

print("  reconfiguring with checkpoint_meta (meta1)")
t0 = time.time()
follower_conn.reconfigure(f'disaggregated=(checkpoint_meta="{ckpt_meta1}")')
setup_pickup_elapsed = time.time() - t0
print(f"  setup pick-up took {setup_pickup_elapsed:.2f}s")
print(f"PERF reconfigure_setup_pickup_secs: {setup_pickup_elapsed:.4f}")

# ----------------------------------------------------------------------
# Phase 3: leader dirties every table and checkpoints (meta2).
# ----------------------------------------------------------------------
print("=" * 70)
print(f"Phase 3: leader mutating {NUM_TABLES} tables (1 insert each)")
print("=" * 70)

commit_ts = 20
t0 = time.time()
for i in range(NUM_TABLES):
    uri = f"table:{TABLE_PREFIX}{i}"
    leader_session.begin_transaction()
    c = leader_session.open_cursor(uri)
    c["k"] = "v"
    c.close()
    leader_session.commit_transaction(f"commit_timestamp={commit_ts}")
    if (i + 1) % 10000 == 0:
        print(f"  mutated {i+1}/{NUM_TABLES}  ({time.time()-t0:.1f}s)")
print(f"  all {NUM_TABLES} tables mutated in {time.time()-t0:.1f}s")

leader_conn.set_timestamp(f"stable_timestamp={commit_ts}")
print("  taking update checkpoint (meta2)")
t0 = time.time()
leader_session.checkpoint()
print(f"  checkpoint completed in {time.time()-t0:.1f}s")

ckpt_meta2 = fetch_checkpoint_meta(leader_conn)

# ----------------------------------------------------------------------
# Phase 4: timed follower pick-up of the updated checkpoint.
# ----------------------------------------------------------------------
print("=" * 70)
print("Phase 4: follower update pick-up (meta2, timed)")
print("=" * 70)

print("  reconfiguring with checkpoint_meta (meta2, timed)")
pickup_t0 = time.time()
follower_conn.reconfigure(f'disaggregated=(checkpoint_meta="{ckpt_meta2}")')
pickup_elapsed = time.time() - pickup_t0
print(f"  RECONFIGURE (update pick-up) took {pickup_elapsed:.2f}s")
print(f"PERF reconfigure_update_pickup_secs: {pickup_elapsed:.4f}")

apply_meta_ms = get_conn_stat(follower_conn, stat.conn.disagg_apply_checkpoint_meta_time)
file_meta_updated = get_conn_stat(follower_conn, stat.conn.disagg_pick_up_file_meta_updated)
print(f"PERF disagg_apply_checkpoint_meta_ms: {apply_meta_ms}")
print(f"PERF disagg_pick_up_file_meta_updated: {file_meta_updated}")

if file_meta_updated < NUM_TABLES:
    raise RuntimeError(
        f"Expected disagg_pick_up_file_meta_updated >= {NUM_TABLES}, got {file_meta_updated}. "
        "Shared file: checkpoint cookies likely did not change (clean trees / no-op pick-up)."
    )

leader_session.close()
leader_conn.close()
follower_conn.close()

print("=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"  num_tables                         = {NUM_TABLES}")
print(f"  follower setup pick-up (meta1)     = {setup_pickup_elapsed:.2f}s")
print(f"  follower update pick-up (meta2)    = {pickup_elapsed:.2f}s")
print(f"  disagg_apply_checkpoint_meta_time  = {apply_meta_ms} ms")
print(f"  disagg_pick_up_file_meta_updated   = {file_meta_updated}")
print(f"  artifacts under                    = {home}")
