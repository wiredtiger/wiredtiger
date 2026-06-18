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
# disagg_many_dhandle_startup_perf.py
#   Disagg follower startup performance benchmark for the many-dhandle case.
#   Measures how long it takes a fresh follower to (a) open and (b) ingest a
#   checkpoint produced by a leader that just created N layered tables. Cost
#   scales primarily with the number of dhandles materialized during pickup.
#
#     Phase 1 (leader):    create N layered tables, light timestamped populate,
#                          checkpoint, capture checkpoint_meta from PALI, close.
#     Phase 2 (follower):  open the same home as a follower (no checkpoint_meta
#                          on the open call)  timed in isolation, then drive
#                          the pickup separately via reconfigure(checkpoint_meta=)
#                           also timed. A brief read workload follows to
#                          exercise first-cursor-open lazy ingest creation.
#
#   Both phases run against the same WT home directory and the same PALI store
#   (kv_home/, auto-created by palite under <home>/). When the follower opens
#   after the leader has closed, disaggregated.local_files_action=delete (the
#   default) wipes the leader's leftover .wt / .wt_ingest files while kv_home/
#   stays put  that's where the checkpoint we're picking up actually lives.
#
#   Env: WT_BUILDDIR must point at the build dir containing
#        ext/page_log/palite/libwiredtiger_palite.so.
#

from runner import *
from wiredtiger import *
from workgen import *
import os, time

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
KEYS_PER_TABLE = 5            # tiny populate so tables are non-empty

PAGE_LOG = "palite"
TABLE_PREFIX = "test_disagg_pickup_"

# ----------------------------------------------------------------------
# Set up home. We use a single WT home for both the leader and the follower:
# the leader writes local files there alongside palite's kv_home/ (the PALI
# backing store). When the follower opens the same dir after the leader has
# closed, disaggregated.local_files_action=delete (the default) wipes the
# leftover .wt / .wt_ingest files, while palite's kv_home/ stays put  that's
# where the checkpoint we're picking up actually lives.
# ----------------------------------------------------------------------
context = Context()
context.parser.add_argument("--num-tables", dest="num_tables", type=int, default=10000,
    help="Number of layered tables the leader creates (default: 10000)")
context.initialize()           # parses all args, creates args.home
home = context.args.home

NUM_TABLES = context.args.num_tables

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
    # tight create loop.
    f"file_manager=(close_handle_minimum=10,close_idle_time=2,close_scan_interval=1),"
    f'extensions=("{ext_path}"=(config="(verbose=0)")),'
    f"disaggregated=(page_log={PAGE_LOG},lose_all_my_data=true,"
)

# ----------------------------------------------------------------------
# Phase 1: leader creates N layered tables, populates a few keys, checkpoints.
# ----------------------------------------------------------------------
print("=" * 70)
print(f"Phase 1: leader creating {NUM_TABLES} layered tables")
print("=" * 70)

leader_conn = wiredtiger_open(
    home, "create," + base_conn_config + 'role="leader")')
leader_session = leader_conn.open_session()

# Initialize timestamps before any writes so commits can use commit_timestamp.
leader_conn.set_timestamp("stable_timestamp=1")

table_cfg = "key_format=S,value_format=S,type=layered,block_manager=disagg"

# Create + populate in a single linear pass. Avoids workgen Op-chain blowup
# at large NUM_TABLES, and creating + writing each table back-to-back keeps
# the dhandle hot in cache for its inserts. We close and reopen the session
# every 1000 tables to release cached dhandles and avoid EMFILE (too many
# open files) when NUM_TABLES is large.
t0 = time.time()
ts = 2
for i in range(NUM_TABLES):
    uri = f"table:{TABLE_PREFIX}{i}"
    leader_session.create(uri, table_cfg)
    # c = leader_session.open_cursor(uri)
    # leader_session.begin_transaction("isolation=snapshot")
    # for k in range(KEYS_PER_TABLE):
    #     c[f"k{k:08d}"] = f"v{k:08d}"
    # leader_session.commit_transaction(f"commit_timestamp={ts:x}")
    ts += 1
    # c.close()
    if (i + 1) % 100 == 0:
        leader_conn.set_timestamp(f"stable_timestamp={ts -1:x}")
        leader_session.checkpoint()
        leader_session.close()
        leader_session = leader_conn.open_session()
    if (i + 1) % 1000 == 0:
        print(f"  created+populated {i+1}/{NUM_TABLES}  ({time.time()-t0:.1f}s)")
        # Release cached dhandles to avoid running out of file descriptors.
        # Yield so the sweep server can grab the dhandle/handle-list locks
        # the create loop has been hammering, and actually expire idle
        # dhandles. Without this, sweep can stall for tens of seconds at a
        # time under heavy schema activity.
        time.sleep(1)
print(f"  all {NUM_TABLES} tables created+populated in {time.time()-t0:.1f}s")

# Push stable to the latest commit so the checkpoint captures every write.
leader_conn.set_timestamp(f"stable_timestamp={ts - 1:x}")

print("  taking checkpoint")
t0 = time.time()
leader_session.checkpoint()
print(f"  checkpoint completed in {time.time()-t0:.1f}s")

# Pull the complete-checkpoint metadata from PALI before closing the leader.
print("  fetching checkpoint_meta from PALI")
page_log = leader_conn.get_page_log(PAGE_LOG)
meta_session = leader_conn.open_session()
(_, _, _, ckpt_meta) = page_log.pl_get_complete_checkpoint_ext(meta_session)
page_log.terminate(meta_session)
meta_session.close()
assert ckpt_meta, "no complete checkpoint metadata returned from PALI"
print(f"  checkpoint_meta length: {len(ckpt_meta)} bytes")

leader_conn.close()
print("  leader closed")

# ----------------------------------------------------------------------
# Phase 2: follower opens with checkpoint_meta. Time wiredtiger_open only.
# ----------------------------------------------------------------------
print("=" * 70)
print("Phase 2: follower picking up the checkpoint")
print("=" * 70)

# Open the follower WITHOUT checkpoint_meta first, so the open call does no
# pickup work  this isolates pure connection-open cost.
follower_open_config = (
    "create," + base_conn_config + 'role="follower")'
)

print("  opening follower connection (no pickup yet, timed)")
open_t0 = time.time()
follower_conn = wiredtiger_open(home, follower_open_config)
open_elapsed = time.time() - open_t0
print(f"  WIREDTIGER_OPEN (no pickup) took {open_elapsed:.2f}s")
# Machine-readable line for the evergreen perf parser.
print(f"PERF wiredtiger_open_no_pickup_secs: {open_elapsed:.4f}")

# Now drive the checkpoint pickup via reconfigure. This matches MongoDB's
# real usage (control plane hands the follower a checkpoint_meta after open)
# and lets us time pickup separately from open.
print("  reconfiguring with checkpoint_meta (pickup, timed)")
pickup_t0 = time.time()
follower_conn.reconfigure(f'disaggregated=(checkpoint_meta="{ckpt_meta}")')
pickup_elapsed = time.time() - pickup_t0
print(f"  RECONFIGURE (pickup) took {pickup_elapsed:.2f}s")
# Machine-readable line for the evergreen perf parser.
print(f"PERF reconfigure_pickup_secs: {pickup_elapsed:.4f}")

# Linearly walk every table on the follower: open cursor (triggers lazy
# ingest creation on first touch), check that the keys we wrote on the
# leader read back, close. We time the whole loop  that's the materialize-
# all-ingest-tables cost, complementing the pickup-only number above.
# print(f"  reading every table on the follower (linear, timed)")
# follower_session = follower_conn.open_session()
# read_t0 = time.time()
# mismatches = 0
# for i in range(NUM_TABLES):
#     uri = f"table:{TABLE_PREFIX}{i}"
#     c = follower_session.open_cursor(uri)
#     for k in range(KEYS_PER_TABLE):
#         key = f"k{k:08d}"
#         expected = f"v{k:08d}"
#         actual = c[key]
#         if actual != expected:
#             mismatches += 1
#     c.close()
#     if (i + 1) % 1000 == 0:
#         print(f"  read {i+1}/{NUM_TABLES}  ({time.time()-read_t0:.1f}s)")
#         # Release cached dhandles to avoid running out of file descriptors.
#         follower_session.close()
#         follower_session = follower_conn.open_session()
# read_elapsed = time.time() - read_t0
# print(f"  read all tables in {read_elapsed:.2f}s ({mismatches} mismatches)")
# print(f"PERF read_all_tables_secs: {read_elapsed:.4f}")
# assert mismatches == 0, f"{mismatches} key/value mismatches on follower"
# follower_session.close()

follower_conn.close()
print("  follower closed")

print("=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"  num_tables                    = {NUM_TABLES}")
print(f"  follower wiredtiger_open      = {open_elapsed:.2f}s   (no pickup)")
print(f"  follower reconfigure pickup   = {pickup_elapsed:.2f}s")
# print(f"  follower read all tables      = {read_elapsed:.2f}s")
print(f"  artifacts under               = {home}")
