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
#
# Tests of the next raw key/value batch API on prefix compressed keys.

import hashlib, wiredtiger, wttest
from wtscenario import make_scenarios

class test_cursor28(wttest.WiredTigerTestCase):
    # Runs of keys sharing a long prefix, alternating with runs of keys sharing almost nothing. The
    # idea is that pages hold a mix of prefix compressed and uncompressed keys.
    nruns = 8
    run_len = 40

    conn_config = 'statistics=(all)'

    tables = [
        ('file', dict(uri='file:test_cursor28.wt')),
        ('table', dict(uri='table:test_cursor28')),
    ]

    sizes = [
        ('batch1', dict(batch_size=1)),
        ('batch7', dict(batch_size=7)),
        ('batch64', dict(batch_size=64)),
    ]

    scenarios = make_scenarios(tables, sizes)

    # Alternates between keys of the form `a_common_prefix_0123` and `a_{digest}`
    def make_keys(self):
        keys = []
        for run in range(self.nruns):
            letter = chr(ord('a') + run)
            if run % 2 == 0:
                keys += [f'{letter}_common_prefix_{i:04d}' for i in range(self.run_len)]
            else:
                digests = (hashlib.sha256(f'{run}/{i}'.encode()).hexdigest()[:32] for i in range(self.run_len))
                keys += [letter + d for d in digests]
        return keys

    def populate(self):
        self.session.create(self.uri,
            'key_format=S,value_format=S,prefix_compression=true,leaf_page_max=4KB')
        rows = {key: f'value{i}' for i, key in enumerate(self.make_keys())}
        cursor = self.session.open_cursor(self.uri)
        for key, value in rows.items():
            cursor[key] = value
        cursor.close()

        # Keys are only prefix compressed when a page is written
        self.session.checkpoint()
        self.assertStatGreaterSoon(wiredtiger.stat.dsrc.rec_prefix_compression_full, 0, uri=self.uri)
        self.reopen_conn()
        return sorted(rows.items())

    def decode(self, raw):
        return raw.rstrip(b'\x00').decode()

    def read_batches(self, cursor):
        batches = []
        while True:
            batch = cursor.next_raw_key_value_batch(self.batch_size)
            if not batch:
                return batches
            batches.append(batch)

    def test_walk(self):
        expected = self.populate()
        cursor = self.session.open_cursor(self.uri)
        batches = self.read_batches(cursor)
        if self.batch_size > 1:
            self.assertGreater(max(len(b) for b in batches), 1)
        rows = [(self.decode(k), self.decode(v)) for batch in batches for k, v in batch]
        self.assertEqual(rows, expected)

    def test_next_after_batch(self):
        expected = self.populate()
        cursor = self.session.open_cursor(self.uri)
        batch = cursor.next_raw_key_value_batch(self.batch_size)
        rows = [(self.decode(k), self.decode(v)) for k, v in batch]
        self.assertEqual(rows, expected[:len(rows)])

        # The cursor must carry on from where the batch stopped.
        for key, value in expected[len(rows):]:
            self.assertEqual(cursor.next(), 0)
            self.assertEqual((cursor.get_key(), cursor.get_value()), (key, value))
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
