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

import os
import shutil
import threading
import time
import wiredtiger
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from rollback_to_stable_util import test_rollback_to_stable_base
from wiredtiger import stat
from wtthread import checkpoint_thread


# test_verify_hs_overlap_recon.py
#
# Deterministic single-process reproduction of the reconciliation duplicate
# observed under the format stress config: the same version [A, B) ends up both
# on the data store page (as a bounded value cell) and in the history store, so
# __verify_key_hs reports "history store stop timestamp B newer than a more
# recent start timestamp A".
#
# Distilled from a captured format trace (file:T00004 key 97, A=12277437,
# B=12279778). The duplicate cannot exist in any single consistent live-tree
# state; it is a checkpoint snapshot that mixes a data store image holding the
# bounded cell [A, B) with a history store image holding the same version with
# its start zeroed -> [0, B). The zeroed start is the form that motivated the
# fix: __verify_key_hs must not report HS stop B against DS start A when the HS
# start has been cleared to 0.
#
#   - V committed at A on every key, then deleted at B on the even keys and
#     evicted while oldest < A, so the page holds a bounded value cell [A, B)
#     (V retained on disk because oldest < A).
#   - oldest is then advanced into (A, B) and stable to B. The [A, B) leaf is
#     already clean, so the checkpoint references it as-is (block reuse) and
#     does not re-reconcile it; the cell keeps its real start A.
#   - A checkpoint with the history_store_checkpoint_delay timing stress starts
#     with checkpoint_timestamp = stable = B. Its data store phase copies the
#     [A, B) cell onto the checkpoint image, then it sleeps before the history
#     store phase.
#   - Inside that delay window a newer value is written over the even keys and
#     the page is evicted. Because oldest > A, V's start is globally visible and
#     the reconcile pinned timestamp is min(oldest, checkpoint_timestamp=B) >= A,
#     so obsolete time window clearing zeros V's start and it lands in the
#     history store as [0, B). The same checkpoint then captures that [0, B)
#     record while its data store image still holds [A, B) -> the same version
#     exists in both, HS [0, B) against DS [A, B).
class test_verify_hs_overlap_recon(test_rollback_to_stable_base):
    format_values = [
        ("column", dict(key_format="r", value_format="S")),
        ("row_integer", dict(key_format="i", value_format="S")),
    ]
    scenarios = make_scenarios(format_values)

    # A handful of keys on a single tiny leaf page. The odd keys stay live so
    # they anchor the release_evict cursor; the even keys carry the delete that
    # produces the bounded [A, B) cell.
    nrows = 20

    def conn_config(self):
        return ("cache_size=50MB,statistics=(all),"
                "timing_stress_for_test=[history_store_checkpoint_delay]")

    def evict(self, uri):
        evict_cur = self.session.open_cursor(uri, None, "debug=(release_evict)")
        self.session.begin_transaction("ignore_prepare=true")
        for i in range(1, self.nrows + 1):
            evict_cur.set_key(self.ds.key(i))
            evict_cur.search()
            if i % 5 == 0:
                evict_cur.reset()
        evict_cur.close()
        self.session.rollback_transaction()

    def write(self, uri, value, ts, start=1, step=1):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(start, self.nrows + 1, step):
            cursor[self.ds.key(i)] = value
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(ts))
        cursor.close()

    def delete_even(self, uri, ts):
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(2, self.nrows + 1, 2):
            cursor.set_key(self.ds.key(i))
            cursor.remove()
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(ts))
        cursor.close()

    def build_skewed_dup(self, uri, value, value2, A, B, C):
        # Build the DS [A, B) + HS A->B duplicate and leave self.conn/self.session
        # pointed at a reopened backup. See the module comment for the mechanism.
        self.ds = SimpleDataSet(self, uri, 0,
            key_format=self.key_format, value_format=self.value_format,
            config="leaf_page_max=4096,memory_page_max=4096")
        self.ds.populate()

        # oldest stays below A for the whole test so V is always required by a
        # reader and reconcile must preserve it. stable starts below A.
        self.conn.set_timestamp("oldest_timestamp=" + self.timestamp_str(1)
            + ",stable_timestamp=" + self.timestamp_str(10))

        # V committed at A on every key, then evicted so it lives on disk.
        self.write(uri, value, A)
        self.evict(uri)

        # Delete the even keys at B, then evict while stable is still below B so
        # the bounded value cell [A, B) is written to disk. The odd keys stay
        # live and anchor the eviction cursor.
        self.delete_even(uri, B)
        self.evict(uri)

        # Advance oldest into (A, B) and stable to B before the checkpoint. The
        # [A, B) leaf is already clean on disk, so the checkpoint references it
        # as-is (block reuse) rather than re-reconciling it, and the cell keeps
        # its real start A. Advancing oldest past A is what arms the zeroing gate
        # for the in-window eviction below: the reconcile pinned timestamp is
        # min(oldest, checkpoint_timestamp=B), and with oldest > A that floor is
        # >= A, so V's start becomes globally visible and is cleared to 0. stable
        # = B means the restart RTS keeps both the [A, B) data store cell and the
        # [0, B) history store record instead of rolling them back.
        self.conn.set_timestamp("oldest_timestamp=" + self.timestamp_str(A + 1)
            + ",stable_timestamp=" + self.timestamp_str(B))

        # One checkpoint with the history store checkpoint delay. Its data store
        # phase copies the [A, B) cell onto the checkpoint image, then it sleeps
        # before checkpointing the history store.
        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done, checkpoint_count_max=1)
        try:
            ckpt.start()

            # Wait until the checkpoint has acquired its snapshot, then let it
            # finish the data store phase and enter the delay window.
            snapshot = 0
            while not snapshot:
                time.sleep(0.5)
                stat_cursor = self.session.open_cursor('statistics:', None, None)
                snapshot = stat_cursor[stat.conn.checkpoint_snapshot_acquired][2]
                stat_cursor.close()
            time.sleep(2)

            # R1 (zero the data store cell). Dirty the leaf through the odd keys
            # so the clean even-key [A, B) cells are re-reconciled through the
            # on-disk-cell path with no new update of their own. With oldest > A
            # the reconcile pinned timestamp is min(oldest, checkpoint=B) >= A,
            # so V's start is globally visible and obsolete clearing zeros it:
            # the freshly written leaf block holds the even keys as [0, B). The
            # checkpoint already captured the pre-R1 [A, B) leaf block for its
            # data store image (block reuse), so that copy keeps the real start.
            self.write(uri, value2, C, start=1, step=2)
            self.evict(uri)

            # R2 (push the zeroed value to the history store). Supersede the even
            # keys with a newer value. Reconcile reads the now-zeroed start 0
            # from the on-disk cell, pairs it with the stop at B, and moves the
            # superseded version into the history store as [0, B). The checkpoint
            # then captures that record in its delayed history store phase, so
            # the same checkpoint holds HS [0, B) against its DS [A, B).
            self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(B + 5))
            self.write(uri, value2, C, start=2, step=2)
            self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(C + 5))
            self.evict(uri)
        finally:
            done.set()
            ckpt.join()

        # The duplicate exists only in the checkpoint the thread took: its data
        # store image holds [A, B) (the leaf evicted while stable < B) while its
        # history store image holds A->B (captured after the in-window eviction
        # pushed V there while stable > B). A fresh checkpoint would re-reconcile
        # the now-stable delete and drop [A, B) from the data store, hiding the
        # overlap, and the live page that holds the still-required V will not
        # evict, so verify cannot get an exclusive handle in this connection.
        #
        # Copy the on-disk checkpoint with a backup cursor and reopen it instead.
        # The backup captures the skewed checkpoint but not the dirty live page,
        # and verify then runs against a clean cache. Both copies are below
        # stable, so the restart RTS keeps them.
        backup_dir = "BACKUP"
        os.makedirs(backup_dir, exist_ok=True)
        bkup_c = self.session.open_cursor("backup:", None, None)
        while bkup_c.next() == 0:
            shutil.copy(bkup_c.get_key(), backup_dir)
        bkup_c.close()

        self.close_conn()
        self.conn = self.setUpConnectionOpen(backup_dir)
        self.session = self.setUpSessionOpen(self.conn)


    def test_verify_hs_overlap_recon(self):
        uri = "table:verify_hs_overlap_recon"
        self.build_skewed_dup(uri, "abcde" * 4, "fghij" * 4, 20, 30, 40)

        # __verify_key_hs walks the on-disk data store against the history store
        # and reports the [A, B) overlap.
        self.session.verify(uri, None)

    def test_rts_dup_consistency(self):
        # Sanity check that rollback to stable handles the DS/HS duplicate
        # consistently. Forcing stable below B makes RTS rewrite both copies:
        # it restores the DS bounded cell to a live V and drops the HS record.
        # If the two were rolled back inconsistently the reads below would
        # diverge; a consistent RTS leaves V live on every key at every read
        # timestamp at or after A.
        uri = "table:verify_hs_overlap_recon"
        value = "abcde" * 4
        A, B, C = 20, 30, 40
        self.build_skewed_dup(uri, value, "fghij" * 4, A, B, C)

        self.conn.set_timestamp(
            "stable_timestamp=" + self.timestamp_str(B - 5) + ",force=true")
        self.conn.rollback_to_stable()

        # delete@B and V2@C are above stable -> rolled back; V@A is live.
        self.check(value, uri, self.nrows, A + 1)
        self.check(value, uri, self.nrows, C + 10)
