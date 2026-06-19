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
# Verify emits a database-level size summary when the 'log_size' verify option is set, and stays
# silent otherwise. The summary is raw constituents only (per-page-type counts and bytes, leaf
# key/value bytes and counts, and a leaf page-size histogram); derived figures such as overhead are
# computed by this test, not the engine.
class test_verify03(test_verbose_base):
    uri = 'table:test_verify03'
    # 32KB leaf / 16KB internal mirrors the MongoDB collection/index page targets.
    params = 'key_format=S,value_format=S,leaf_page_max=32KB,internal_page_max=16KB'
    nrecords = 10000

    # Pull the raw constituent fields out of verify's "Size metrics" message. All fields are on the
    # single always-emitted line. Overhead and the uncompressed total are derived here, mirroring the
    # downstream analysis the engine deliberately leaves to the consumer.
    def parse_summary(self, output):
        def find(pattern):
            m = re.search(pattern, output)
            self.assertTrue(m is not None, 'missing "%s" in verify output' % pattern)
            return int(m.group(1))
        s = dict(
            leaf=find(r'leaf pages=(\d+)'),
            internal=find(r'internal pages=(\d+)'),
            overflow=find(r'overflow pages=(\d+)'),
            leaf_bytes=find(r'leaf bytes=(\d+)'),
            internal_bytes=find(r'internal bytes=(\d+)'),
            overflow_bytes=find(r'overflow bytes=(\d+)'),
            key=find(r'key bytes=(\d+)'),
            value=find(r'value bytes=(\d+)'),
            key_count=find(r'key count=(\d+)'),
            value_count=find(r'value count=(\d+)'),
        )
        s['total'] = s['leaf_bytes'] + s['internal_bytes'] + s['overflow_bytes']
        s['overhead'] = s['total'] - (s['key'] + s['value'])
        return s

    # Pull the leaf page-size histogram as an ordered list of per-bucket page counts. The buckets run
    # low to high with the final entry being the ">=maxleafpage" bucket; the sum equals the leaf page
    # count.
    def parse_histogram(self, output):
        line = next((l for l in output.splitlines() if 'Leaf page-size histogram' in l), None)
        self.assertIsNotNone(line, 'missing leaf page-size histogram in verify output')
        return [int(c) for c in re.findall(r'B:(\d+)', line)]

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
    # everything it printed to stdout via the application's message handler. The size summary is
    # always emitted when log_size is set; it is not gated behind verbose verify.
    def verify_capture(self, log_size):
        self.cleanStdout()
        conn = self.wiredtiger_open(self.home, '')
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
    # above.
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

        output = self.verify_capture(log_size=True)
        s = self.parse_summary(output)

        # Overhead (derived) is non-negative and a small fraction of well-packed user data.
        self.assertGreaterEqual(s['overhead'], 0)

        # A single append-only btree at this scale: many leaf pages, no overflow, multiple GB. The
        # 'u' value format stores exactly valuesize bytes per record (no trailing NUL).
        self.assertGreater(s['leaf'], 1000)
        self.assertEqual(s['overflow'], 0)
        self.assertGreaterEqual(s['total'], 4 * 1024 * 1024 * 1024)
        self.assertGreaterEqual(s['value'], nrecords * valuesize)
        self.assertEqual(s['key_count'], s['value_count'])

        # Append-only leaves pack tightly: the histogram weight sits in the high buckets and overhead
        # is a small fraction of user data, unlike the underfull pathology.
        hist = self.parse_histogram(output)
        self.assertEqual(sum(hist), s['leaf'])
        # The top two in-range buckets (>=75% of maxleafpage) hold the bulk of the pages.
        self.assertGreaterEqual(hist[-3] + hist[-2], int(0.8 * s['leaf']))
        self.assertLess(s['overhead'] * 100 // (s['key'] + s['value']), 25)

    def test_verify_size_metrics(self):
        self.populate_underfull()

        # The size summary is always emitted (not gated behind verbose) as two messages: the scalar
        # constituents and the leaf page-size histogram.
        output = self.verify_capture(log_size=True)
        for expected in ['Size metrics:', 'leaf pages=', 'internal pages=', 'overflow pages=',
          'leaf bytes=', 'internal bytes=', 'overflow bytes=', 'key bytes=', 'value bytes=',
          'key count=', 'value count=', 'Leaf page-size histogram']:
            self.assertIn(expected, output)
        # The retired derived figures and the on-disk size must no longer be emitted by the engine.
        for unexpected in ['compressed bytes=', 'compression ratio=', 'leaf fullness=',
          'overhead bytes=', 'fullness deciles=', 'prefix-compression savings=']:
            self.assertNotIn(unexpected, output)

        # The scalar constituents are a single line, prefixed with the object's (file) URI.
        line = next(l for l in output.splitlines() if 'Size metrics:' in l)
        self.assertTrue(line.startswith('file:test_verify03.wt: Size metrics:'),
          'expected the object URI at the start of the message: %s' % line)

        s = self.parse_summary(output)
        # Byte accounting is self-consistent: derived overhead is non-negative, and uncompressed
        # bytes are at least the counted user data.
        self.assertGreaterEqual(s['overhead'], 0)
        self.assertGreaterEqual(s['total'], s['key'] + s['value'])
        # Non-empty values mean one value cell per key.
        self.assertEqual(s['key_count'], s['value_count'])
        self.assertGreater(s['key_count'], 0)
        # The histogram covers every leaf page exactly once.
        self.assertEqual(sum(self.parse_histogram(output)), s['leaf'])

        # Without the option, no summary must appear.
        output = self.verify_capture(log_size=False)
        self.assertFalse('Size metrics' in output,
          'size summary should only be emitted when log_size=true is set')

    # Delete 99% of the keys, leaving a large population of near-empty leaf pages that WiredTiger
    # never merges back together. This is the pathology the page-size histogram exists to surface:
    # the pages are uniformly underfull, concentrated in the smallest bucket.
    def test_verify_size_underfull(self):
        self.populate_underfull(keep_mod=100, nrecords=200000)

        output = self.verify_capture(log_size=True)
        s = self.parse_summary(output)

        # Byte accounting stays self-consistent regardless of tree shape.
        self.assertGreaterEqual(s['overhead'], 0)

        # The tree is built from many leaf pages, and after the deletes they are heavily underfull.
        self.assertGreater(s['leaf'], 50)

        # The underfull pages are a uniform population, not a few outliers: the smallest bucket
        # (<1/8 of maxleafpage) holds the vast majority, and the buckets sum to the leaf page count.
        hist = self.parse_histogram(output)
        self.assertEqual(sum(hist), s['leaf'])
        self.assertGreaterEqual(hist[0], int(0.8 * s['leaf']))

    # An overflow item's payload is counted against value bytes (via the overflow page), so it is not
    # mistaken for overhead, keeping derived overhead a small fraction of a well-packed tree.
    def test_verify_size_overflow(self):
        nrecords, valuesize = 500, 2000
        self.populate_overflow(nrecords, valuesize)

        output = self.verify_capture(log_size=True)
        s = self.parse_summary(output)

        # Large values must have produced overflow pages.
        self.assertGreater(s['overflow'], 0)
        # The overflow payloads (valuesize + NUL for the S format) dominate user value bytes; if they
        # were miscounted as overhead, derived overhead would balloon past the user data.
        self.assertGreaterEqual(s['value'], nrecords * valuesize)
        self.assertGreaterEqual(s['overhead'], 0)
        self.assertLess(s['overhead'], s['key'] + s['value'])
        # Each record contributes one key and one value, including overflow values.
        self.assertEqual(s['value_count'], nrecords)
        self.assertEqual(s['key_count'], nrecords)
