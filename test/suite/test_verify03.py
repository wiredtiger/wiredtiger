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

import re
import wttest
from test_verbose01 import test_verbose_base

# test_verify03.py
# Verify emits a database-level size summary (overhead byte breakdown, on-disk compressed size and
# compression ratio, and page fullness) when the 'log_size' verify option is set, and stays silent
# otherwise.
class test_verify03(test_verbose_base):
    uri = 'table:test_verify03'
    # 32KB leaf / 16KB internal mirrors the MongoDB collection/index page targets.
    params = 'key_format=S,value_format=S,leaf_page_max=32KB,internal_page_max=16KB'
    nrecords = 10000

    # Pull the numeric fields out of verify's output. 'data', 'total' and 'compressed' come from the
    # always-on API message; the per-page-type and overhead fields come from the verbose detail
    # message, so a full parse requires output captured with verbose verify enabled.
    def parse_summary(self, output):
        def find(pattern):
            m = re.search(pattern, output)
            self.assertTrue(m is not None, 'missing "%s" in verify output' % pattern)
            return int(m.group(1))
        return dict(
            data=find(r'data bytes=(\d+)'),
            total=find(r'total bytes=(\d+)'),
            compressed=find(r'compressed bytes=(\d+)'),
            leaf=find(r'leaf pages=(\d+)'),
            internal=find(r'internal pages=(\d+)'),
            overflow=find(r'overflow pages=(\d+)'),
            key=find(r'key bytes=(\d+)'),
            value=find(r'value bytes=(\d+)'),
            overhead=find(r'overhead bytes=(\d+)'),
        )

    # Pull the average leaf fullness (from the API message) and the leaf decile histogram (decile ->
    # page count, from the verbose detail message). The leaf decile list runs from its label to the
    # internal decile label.
    def parse_leaf_fullness(self, output):
        avg = None
        m = re.search(r'leaf fullness=([\d.]+)%', output)
        if m:
            avg = float(m.group(1))
        buckets = {}
        block = re.search(
            r'leaf fullness deciles=(.*?)(?:, internal fullness deciles=|\Z)', output, re.S)
        if block:
            for bm in re.finditer(r'(\d+)%:\s*(\d+)', block.group(1)):
                buckets[int(bm.group(1)) // 10] = int(bm.group(2))
        return avg, buckets

    # Build a tree spanning many leaf pages, then delete most of the keys so the pages are left
    # underfull (WiredTiger does not merge pages back together). Every keep_mod-th key survives, so
    # keep_mod=10 deletes ~90% and keep_mod=100 deletes ~99%.
    def populate_underfull(self, keep_mod=10, nrecords=None):
        if nrecords is None:
            nrecords = self.nrecords
        self.close_conn()
        conn = self.wiredtiger_open(self.home, 'create')
        session = conn.open_session()
        session.create(self.uri, self.params)

        cursor = session.open_cursor(self.uri)
        value = 'v' * 100
        for i in range(nrecords):
            cursor['key%08d' % i] = value
        cursor.close()
        session.checkpoint()

        cursor = session.open_cursor(self.uri)
        for i in range(nrecords):
            if i % keep_mod != 0:
                cursor.set_key('key%08d' % i)
                self.assertEqual(cursor.remove(), 0)
        cursor.close()
        session.checkpoint()
        conn.close()

    # Run verify against a fresh connection, optionally passing the 'log_size' option, and return
    # everything it printed to stdout via the application's message handler. With verbose=True the
    # connection enables the verify verbose category, which surfaces the detailed size message.
    def verify_capture(self, log_size, verbose=False):
        self.cleanStdout()
        conn = self.wiredtiger_open(self.home, 'verbose=[verify]' if verbose else '')
        session = conn.open_session()
        config = 'log_size=true' if log_size else None
        self.assertEqual(session.verify(self.uri, config), 0)
        output = self.readStdout(200 * 1024 * 1024)
        conn.close()
        self.cleanStdout()
        return output

    # Populate with large values that exceed the leaf-value limit, forcing overflow pages.
    def populate_overflow(self, nrecords, valuesize):
        self.close_conn()
        conn = self.wiredtiger_open(self.home, 'create,statistics=(all)')
        session = conn.open_session()
        session.create(self.uri, 'key_format=S,value_format=S,leaf_page_max=4KB,internal_page_max=4KB')
        cursor = session.open_cursor(self.uri)
        value = 'v' * valuesize
        for i in range(nrecords):
            cursor['key%08d' % i] = value
        cursor.close()
        session.checkpoint()
        conn.close()

    # Append-only workload: a single btree filled with monotonically increasing keys, never updated
    # or deleted. Leaf pages pack tightly, the opposite end of the spectrum from the underfull tree
    # above. No block compressor is configured, so the on-disk database is close to the uncompressed
    # image size and the workload genuinely materializes the target size on disk.
    def populate_append(self, nrecords, valuesize):
        self.close_conn()
        conn = self.wiredtiger_open(self.home, 'create,cache_size=2GB')
        session = conn.open_session()
        session.create(self.uri,
          'key_format=Q,value_format=u,leaf_page_max=32KB,internal_page_max=16KB')
        cursor = session.open_cursor(self.uri)
        value = b'\xa5' * valuesize
        for i in range(nrecords):
            cursor[i + 1] = value
        cursor.close()
        session.checkpoint()
        conn.close()

    # Build a ~5GB single btree from an append-only workload. 4KB values keep every record on-page
    # (no overflow) while the tree spans hundreds of thousands of tightly-packed leaf pages, so this
    # exercises the size summary at scale and at the low-overhead end of the spectrum.
    @wttest.longtest('append-only workload building a ~5GB single-btree database')
    def test_verify_size_append_5gb(self):
        valuesize = 4096
        nrecords = (5 * 1024 * 1024 * 1024) // valuesize

        self.populate_append(nrecords, valuesize)

        output = self.verify_capture(log_size=True, verbose=True)
        s = self.parse_summary(output)

        # Byte accounting stays self-consistent at scale.
        self.assertEqual(s['overhead'], s['total'] - (s['key'] + s['value']))

        # A single append-only btree at this scale: many leaf pages, no overflow, multiple GB. The
        # 'u' value format stores exactly valuesize bytes per record (no trailing NUL).
        self.assertGreater(s['leaf'], 1000)
        self.assertEqual(s['overflow'], 0)
        self.assertGreaterEqual(s['total'], 4 * 1024 * 1024 * 1024)
        self.assertGreaterEqual(s['value'], nrecords * valuesize)

        # Append-only leaves pack tightly: fullness is high and overhead is a small fraction of user
        # data, unlike the underfull pathology.
        avg, buckets = self.parse_leaf_fullness(output)
        self.assertIsNotNone(avg)
        self.assertGreaterEqual(avg, 60)
        self.assertEqual(sum(buckets.values()), s['leaf'])
        self.assertLess(s['overhead'] * 100 // (s['key'] + s['value']), 25)

    def test_verify_size_metrics(self):
        self.populate_underfull()

        # The always-on API message carries only data bytes, total bytes and leaf fullness. It is a
        # single line and must not leak the detailed breakdown or the histogram.
        api = self.verify_capture(log_size=True)
        for expected in ['Size metrics (uncompressed image):', 'data bytes=', 'total bytes=',
          'compressed bytes=', 'compression ratio=', 'leaf fullness=', 'overhead=']:
            self.assertIn(expected, api)
        for unexpected in ['Size metrics detail:', 'key bytes=', 'overhead bytes=',
          'fullness deciles=']:
            self.assertNotIn(unexpected, api)
        api_line = next(l for l in api.splitlines() if 'Size metrics (uncompressed image):' in l)
        # The verified object's URI is logged at the start of the message (the underlying file URI).
        self.assertTrue(api_line.startswith('file:test_verify03.wt: Size metrics'),
          'expected the object URI at the start of the message: %s' % api_line)
        for expected in ['data bytes=', 'total bytes=', 'compressed bytes=', 'compression ratio=',
          'leaf fullness=', 'overhead=']:
            self.assertIn(expected, api_line)
        # The compression ratio is reported to two decimals followed by 'x'.
        self.assertIsNotNone(re.search(r'compression ratio=\d+\.\d{2}x', api_line))

        # With verbose verify enabled, a second message provides the detailed breakdown and the
        # fullness histograms.
        output = self.verify_capture(log_size=True, verbose=True)
        for expected in ['Size metrics detail:', 'leaf pages=', 'key bytes=', 'value bytes=',
          'overhead bytes=', 'avg internal fullness=', 'leaf fullness deciles=',
          'internal fullness deciles=']:
            self.assertIn(expected, output)
        # The detail is a single line.
        detail_line = next(l for l in output.splitlines() if 'Size metrics detail:' in l)
        for expected in ['leaf pages=', 'overhead bytes=', 'leaf fullness deciles=']:
            self.assertIn(expected, detail_line)

        # The byte breakdown must be self-consistent: overhead is whatever is not user data, and the
        # API data-bytes figure equals key + value.
        s = self.parse_summary(output)
        self.assertEqual(s['overhead'], s['total'] - (s['key'] + s['value']))
        self.assertGreaterEqual(s['total'], s['key'] + s['value'])
        self.assertEqual(s['data'], s['key'] + s['value'])
        # A non-empty tree has a non-zero on-disk footprint. With no block compressor configured the
        # on-disk size is the uncompressed image plus block headers rounded to the allocation unit,
        # so it is in the same ballpark as the uncompressed total rather than dramatically smaller.
        self.assertGreater(s['compressed'], 0)

        # Without the option, neither message must appear.
        output = self.verify_capture(log_size=False, verbose=True)
        self.assertFalse('Size metrics' in output,
          'size summary should only be emitted when log_size=true is set')

    # Delete 99% of the keys, leaving a large population of near-empty leaf pages that WiredTiger
    # never merges back together. This is the pathology the fullness histogram exists to surface:
    # the average alone says "bad", but the histogram shows the pages are uniformly underfull.
    def test_verify_size_underfull(self):
        self.populate_underfull(keep_mod=100, nrecords=200000)

        output = self.verify_capture(log_size=True, verbose=True)
        s = self.parse_summary(output)

        # Byte accounting stays self-consistent regardless of tree shape.
        self.assertEqual(s['overhead'], s['total'] - (s['key'] + s['value']))

        # The tree is built from many leaf pages, and after the deletes they are heavily underfull.
        self.assertGreater(s['leaf'], 50)
        avg, buckets = self.parse_leaf_fullness(output)
        self.assertIsNotNone(avg)
        self.assertLessEqual(avg, 10)

        # The signal the histogram adds over the average: the underfull pages are a uniform
        # population, not a few outliers. The bottom decile (<10% full) holds the vast majority.
        self.assertIn(0, buckets)
        self.assertGreaterEqual(buckets[0], int(0.8 * s['leaf']))
        # All histogram buckets must sum to the reported leaf page count.
        self.assertEqual(sum(buckets.values()), s['leaf'])

    # An overflow item's payload is user data, not overhead: it must be counted against key/value
    # bytes (via the overflow page), keeping overhead a small fraction of a well-packed tree.
    def test_verify_size_overflow(self):
        nrecords, valuesize = 500, 2000
        self.populate_overflow(nrecords, valuesize)

        output = self.verify_capture(log_size=True, verbose=True)
        s = self.parse_summary(output)

        # Large values must have produced overflow pages.
        self.assertGreater(s['overflow'], 0)
        # The byte-accounting invariant holds.
        self.assertEqual(s['overhead'], s['total'] - (s['key'] + s['value']))
        # The overflow payloads (valuesize + NUL for the S format) dominate user value bytes; if
        # they were miscounted as overhead, overhead would balloon past the user data.
        self.assertGreaterEqual(s['value'], nrecords * valuesize)
        self.assertLess(s['overhead'], s['key'] + s['value'])
