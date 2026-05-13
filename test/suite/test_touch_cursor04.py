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
# [TEST_TAGS]
# cursors
# [END_TAGS]

# test_touch_cursor04.py
# Concurrent touch + read workload.
#
# Two writer-less threads share a populated table:
#   - thread A repeatedly opens touch cursors and fires warmup hints.
#   - thread B opens normal read cursors and reads the same keys.
# The threads run for a fixed wall-clock window. Both must complete with
# zero exceptions and zero corrupt reads (every key must read back its
# matching value). This exercises:
#   - Concurrent WT_GEN_SPLIT enter/leave by multiple sessions.
#   - The palite process-static warm-set under mutex contention.
#   - The cursor cache short-circuit refusal for touch cursors under
#     concurrent open/close churn.

import json, random, threading, time, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios


@disagg_test_class
class test_touch_cursor04(wttest.WiredTigerTestCase, DisaggConfigMixin):

    nitems = 5_000
    duration_s = 4.0
    n_touchers = 2
    n_readers = 2

    disagg_storages = gen_disagg_storages('test_touch_cursor04', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    disagg_config = 'touch_sim_enabled=true,touch_sim_warmup_ms=1'

    conn_base_config = (
        'transaction_sync=(enabled,method=fsync),'
        'statistics=(all),'
        'cache_size=8MB,'
        'disaggregated=(page_log=palite),'
    )

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader"),'

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _populate(self, uri):
        common = (
            'key_format=S,value_format=S,block_manager=disagg,'
            'allocation_size=512,leaf_page_max=1KB,internal_page_max=512'
        )
        self.session.create(uri, common)
        c = self.session.open_cursor(uri)
        for i in range(self.nitems):
            c[f'k{i:06d}'] = json.dumps({'i': i, 'pad': 'x' * 64})
        c.close()
        self.session.checkpoint()

    # ---- worker bodies -------------------------------------------------

    def _toucher(self, uri, deadline, rng_seed, errors):
        rng = random.Random(rng_seed)
        try:
            sess = self.conn.open_session()
            try:
                while time.perf_counter() < deadline:
                    c = sess.open_cursor(uri, None,
                                        'touch=(enabled=true,action=warmup)')
                    try:
                        for _ in range(8):
                            i = rng.randrange(self.nitems)
                            c.set_key(f'k{i:06d}')
                            rc = c.search()
                            if rc != wiredtiger.WT_NOTFOUND:
                                raise AssertionError(
                                    f'toucher: search returned {rc}, expected '
                                    f'WT_NOTFOUND')
                    finally:
                        c.close()
            finally:
                sess.close()
        except BaseException as e:
            errors.append(('toucher', e))

    def _reader(self, uri, deadline, rng_seed, errors):
        rng = random.Random(rng_seed)
        try:
            sess = self.conn.open_session()
            try:
                c = sess.open_cursor(uri)
                try:
                    while time.perf_counter() < deadline:
                        i = rng.randrange(self.nitems)
                        c.set_key(f'k{i:06d}')
                        rc = c.search()
                        if rc != 0:
                            raise AssertionError(
                                f'reader: search({i}) returned {rc}')
                        v = json.loads(c.get_value())
                        if v.get('i') != i:
                            raise AssertionError(
                                f'reader: key {i} returned value with i={v.get("i")}')
                finally:
                    c.close()
            finally:
                sess.close()
        except BaseException as e:
            errors.append(('reader', e))

    # ---- tests ---------------------------------------------------------

    def test_concurrent_touch_and_read(self):
        uri = 'file:touch_concurrent.wt'
        self._populate(uri)

        deadline = time.perf_counter() + self.duration_s
        errors = []

        threads = []
        for i in range(self.n_touchers):
            t = threading.Thread(
                target=self._toucher,
                args=(uri, deadline, 0x100 + i, errors),
                name=f'toucher-{i}')
            threads.append(t)
        for i in range(self.n_readers):
            t = threading.Thread(
                target=self._reader,
                args=(uri, deadline, 0x200 + i, errors),
                name=f'reader-{i}')
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=self.duration_s + 60.0)
            self.assertFalse(t.is_alive(),
                             f'thread {t.name} did not finish in time')

        for who, e in errors:
            self.fail(f'{who} thread failed: {e!r}')
