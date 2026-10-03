#!/usr/bin/env python3
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

# A reconciliation that skips writing must not hand the victim cache an image
# whose write generation does not belong to the block it points at. The
# skip-write path keeps the previous block (page id and LSN) but builds a
# fresh in-memory image; stamping that image with a newly allocated write
# generation pairs old block identity with the generation of a write that
# never happened. The fix stamps the retained image of a skipped write with
# the backing block write generation. This test drives the machinery through
# rounds of obsolete-row skip-writes, victim-cache publication and
# cache-served reads, so the consistency assertions at each publication and
# read hold it.

import wiredtiger, wttest
from helper_disagg import DisaggSizeTestMixin, disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios


@disagg_test_class
class test_layered_victim_cache_skip_write_gen(DisaggSizeTestMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_config = (
        'disaggregated=(role="leader",lose_all_my_data=true),'
        'cache_size=2GB,statistics=(all),precise_checkpoint=true,'
        # A delete leaves a page smaller than the delta describing it, so allow
        # a delta to exceed the image it applies to; otherwise these pages are
        # rewritten whole and never come back from disk empty.
        'page_delta=(delta_pct=1000,leaf_page_delta=true),'
        # Counting rebuilt pages depends on the order pages are reconciled in;
        # pin it rather than let the parallel_checkpoint hook reconcile leaf
        # pages out of order.
        'checkpoint_threads=1'
    )
    disagg_config = 'victim_cache_max_entries=10000'

    uri = 'layered:' + test_name
    table_config = 'key_format=S,value_format=S,leaf_page_max=4KB'

    nrows = 10
    value = 'A' * 1024
    pump_rounds = 40

    def key(self, i):
        return 'key{:08d}'.format(i)

    def evict_page(self, key, read_ts=None):
        evict = self.session.open_cursor(self.uri, None, 'debug=(release_evict)')
        cfg = None if read_ts is None else 'read_timestamp=' + self.timestamp_str(read_ts)
        self.session.begin_transaction(cfg)
        evict.set_key(key)
        evict.search_near()
        evict.reset()
        evict.close()
        self.session.rollback_transaction()

    def read_page(self, key):
        c = self.session.open_cursor(self.uri)
        c.set_key(key)
        self.assertEqual(c.search(), 0)
        c.close()

    def test_skip_write_gen_survives_victim_cache(self):
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        self.session.create(self.uri, self.table_config)

        # Persist a full image: this block is what a later skip-write retains.
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[self.key(i)] = self.value
        c.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        # Delete everything and make it stable, but leave oldest behind, so the
        # deletes are still worth recording as deltas.
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c.set_key(self.key(i))
            c.remove()
        c.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()

        deltas = self.get_conn_stat(stat.conn.rec_page_delta_leaf)
        self.assertGreater(deltas, 0, 'the deletes were not recorded as deltas')

        # Drop the now-clean pages, then make the deletes obsolete and read the
        # pages back: they rebuild with no rows left on them. Reads may be
        # served by the victim cache, so do not assert on delta-read counts.
        for i in range(self.nrows):
            self.evict_page(self.key(i), read_ts=10)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(20))
        for i in range(self.nrows):
            c = self.session.open_cursor(self.uri)
            c.set_key(self.key(i))
            c.search_near()
            c.close()

        # Rewrite every key and checkpoint it: each on-page image now holds its
        # final content, and the checkpoint block metadata anchors the page to
        # its block. Nothing after this point checkpoints the table, so each
        # page block identity is final from here on.
        self.session.begin_transaction()
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[self.key(i)] = 'B' * 1024
        c.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(30))
        self.session.checkpoint()

        # Pump write generations through the caches. Each round adds and
        # removes one throwaway key per page in a single transaction; once it
        # is committed and oldest moves past it, the row is obsolete everywhere,
        # so evicting the page reconciles it with nothing durable to write: the
        # write is skipped and a rebuilt image is retained. The read that
        # follows consumes the published victim-cache entry, loading that image
        # back into memory so the next round repeats on top of it. The final
        # round skips its read: the published entries stay in the victim cache
        # for verify.
        for round_num in range(self.pump_rounds):
            ts = 40 + 10 * round_num
            self.session.begin_transaction()
            c = self.session.open_cursor(self.uri)
            for i in range(self.nrows):
                c[self.key(i) + 'x'] = 'C' * 10
                c.set_key(self.key(i) + 'x')
                c.remove()
            c.close()
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
            self.conn.set_timestamp(
                'oldest_timestamp=' + self.timestamp_str(ts + 9) +
                ',stable_timestamp=' + self.timestamp_str(ts + 9))
            for i in range(self.nrows):
                self.evict_page(self.key(i))
            if round_num == self.pump_rounds - 1:
                break
            for i in range(self.nrows):
                self.read_page(self.key(i))

        # Verify reads the tree back through the block identities the retained
        # images claim: the images must carry those blocks' write generations,
        # not generations of writes that never happened. Verify takes the
        # stable dhandle exclusively, so run it on a fresh session and retry
        # while exclusive access is busy. Verify the stable file directly: a
        # checkpoint-named handle bypasses the caches on read, which would
        # defeat the check.
        stable_uri = 'file:' + self.test_name + '.wt_stable'
        verify_session = self.conn.open_session()
        self.retryEBUSY(verify_session, lambda: verify_session.verify(stable_uri))
        verify_session.close()

        # The rewritten values are still all there.
        for i in range(self.nrows):
            c = self.session.open_cursor(self.uri)
            c.set_key(self.key(i))
            self.assertEqual(c.search(), 0)
            self.assertEqual(c.get_value(), 'B' * 1024)
            c.close()
