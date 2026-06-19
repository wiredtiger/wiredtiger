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

import threading, time
import wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_stepup11.py
# Reproduce the step-up ingest-clear eviction crash from inside the engine, with no external pinning
# transaction. The failpoint_disagg_ingest_clear timing stress recreates, during a real step-up, the
# four conditions the crash requires:
#   1. race: one drain worker's clear pins the oldest id and holds it open;
#   2. value-loss: the value behind one sibling clear tombstone is dropped (a lone tombstone);
#   3. cap-bypass: the step-up retention cap is lifted so the drained ingest values are visible;
#   4. select-non-visible: eviction selects the non-globally-visible tombstone instead of restoring
#      it (eviction normally skips updates newer than the pinned id).
# Concurrently evicting the sibling's cleared ingest page then reconciles a lone, non-globally-
# visible tombstone with no on-disk value -- the crash. The non-transactional clear truncate (the
# fix) makes the tombstones globally visible, so it is safe.
@disagg_test_class
class test_layered_stepup11(wttest.WiredTigerTestCase):
    conn_base_config = ',create,cache_size=10GB,statistics=(all),' \
        'statistics_log=(wait=1,json=true,on_close=true),' \
        'disaggregated=(lose_all_my_data=true),precise_checkpoint=true,' \
        'timing_stress_for_test=[failpoint_disagg_ingest_clear],'

    disagg_storages = gen_disagg_storages('test_layered_stepup11', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    create_config = 'key_format=S,value_format=S'
    ntables = 2
    nitems = 8

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + \
            'disaggregated=(role="leader",drain_threads=2)'

    def uri(self, i):
        return f'layered:test_layered_stepup11_{i}'

    def ingest_uri(self, i):
        return f'file:test_layered_stepup11_{i}.wt_ingest'

    def force_evict(self):
        # Wait for the drains and the sibling clear to finish (so the ingest pages keep their cleared
        # keys in memory), then evict the cleared pages while the pinning worker still holds the
        # oldest id. Tolerate transient errors from racing the drain workers.
        time.sleep(1.5)
        deadline = time.time() + 1.5
        while time.time() < deadline:
            session = self.conn.open_session('')
            for i in range(self.ntables):
                try:
                    evict = session.open_cursor(self.ingest_uri(i), None, 'debug=(release_evict)')
                except wiredtiger.WiredTigerError:
                    continue
                for k in range(self.nitems):
                    try:
                        evict.set_key(str(k))
                        evict.search()
                        evict.reset()
                    except wiredtiger.WiredTigerError:
                        break
                evict.close()
            session.close()

    def test_worker_vs_worker_clear_evict(self):
        ts = 10

        # Write as a follower so the keys live only in the ingest tables; values are timestamped, as
        # required for a garbage-collect (ingest) btree.
        for i in range(self.ntables):
            self.session.create(self.uri(i), self.create_config)
        self.conn.reconfigure('disaggregated=(role="follower")')

        write_session = self.conn.open_session('')
        write_session.begin_transaction()
        for i in range(self.ntables):
            cursor = write_session.open_cursor(self.uri(i))
            for k in range(self.nitems):
                cursor[str(k)] = 'value' + str(k)
            cursor.close()
        write_session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
        write_session.close()

        # Advance oldest as well, so once the step-up cap is bypassed the written values are globally
        # visible: the cleared page then has visible progress and eviction proceeds.
        self.conn.set_timestamp(
            f'oldest_timestamp={self.timestamp_str(ts)},stable_timestamp={self.timestamp_str(ts)}')

        evictor = threading.Thread(target=self.force_evict)
        evictor.start()
        self.conn.reconfigure('disaggregated=(role="leader")')
        evictor.join()

        for i in range(self.ntables):
            check_cursor = self.session.open_cursor(self.uri(i))
            count = 0
            while check_cursor.next() == 0:
                count += 1
            self.assertEqual(count, self.nitems)
            check_cursor.close()
