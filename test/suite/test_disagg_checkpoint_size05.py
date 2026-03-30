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

import re, wttest
from wiredtiger import stat
from helper_disagg import disagg_test_class

# test_disagg_checkpoint_size05.py
#   Test that block_size statistic reflects the checkpoint size for disaggregated storage.
#   For disaggregated tables there is no underlying file, so block_size is sourced from the
#   most recent checkpoint size. The stat is updated via two code paths that are both exercised
#   here:
#
#     Slow path  -- statistics=(all) opens the dhandle and reads the checkpoint size from the
#                   block manager handle, which is updated at every checkpoint.
#     Fast path  -- statistics=(size) avoids opening the dhandle and reads the size directly
#                   from the checkpoint entry in the file's metadata.
#
#   At startup, the block manager handle is initialised from the checkpoint metadata so the
#   slow-path stat is also correct before the first new checkpoint is taken after a restart.
@disagg_test_class
class test_disagg_checkpoint_size05(wttest.WiredTigerTestCase):

    uri_base = "test_disagg_ckpt_size05"
    conn_config = 'disaggregated=(role="leader",lose_all_my_data=true)'
    uri = "layered:" + uri_base
    stable_uri = "file:" + uri_base + ".wt_stable"

    def insert_rows(self, n, value='x', start=0, uri=None):
        cursor = self.session.open_cursor(uri or self.uri)
        for i in range(start, start + n):
            cursor[f'key{i:08d}'] = value * 100
        cursor.close()

    # Read block_size from a statistics cursor using the slow path that opens the dhandle.
    def get_block_size_slow(self):
        cstat = self.session.open_cursor('statistics:' + self.stable_uri, None, 'statistics=(all)')
        sz = cstat[stat.dsrc.block_size][2]
        cstat.close()
        return sz

    # Read block_size via statistics=(size) using the fast path that reads directly from metadata.
    def get_block_size_fast(self):
        cstat = self.session.open_cursor('statistics:' + self.stable_uri, None, 'statistics=(size)')
        sz = cstat[stat.dsrc.block_size][2]
        cstat.close()
        return sz

    # Read the checkpoint size out of the raw metadata string (ground truth).
    def get_ckpt_size_from_meta(self):
        mc = self.session.open_cursor('metadata:')
        mc.set_key(self.stable_uri)
        self.assertEqual(mc.search(), 0)
        sizes = re.findall(r',size=(\d+),', mc.get_value())
        mc.close()
        self.assertGreater(len(sizes), 0, "No size= found in checkpoint metadata")
        return int(sizes[-1])


    # Test that both paths must return the same value, and it must match the raw metadata.
    def test_slow_and_fast_path_agree_with_metadata(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(1000)
        self.session.checkpoint()

        meta = self.get_ckpt_size_from_meta()
        self.assertEqual(self.get_block_size_fast(), meta,
            f"statistics=(size) fast path should match metadata size ({meta})")
        self.assertEqual(self.get_block_size_slow(), meta,
            f"statistics=(all) slow path should match metadata size ({meta})")

    # Test block_size grows after additional data is inserted and a new checkpoint is taken.
    def test_block_size_increases_with_data(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(500)
        self.session.checkpoint()
        size_first = self.get_block_size_fast()

        self.insert_rows(1000, start=500)
        self.session.checkpoint()
        fast_second = self.get_block_size_fast()
        meta_second = self.get_ckpt_size_from_meta()

        self.assertGreater(fast_second, size_first,
            f"fast-path block_size should increase after inserting more data")
        self.assertEqual(fast_second, meta_second,
            f"fast path ({fast_second}) should match metadata ({meta_second}) after second checkpoint")
        self.assertEqual(self.get_block_size_slow(), meta_second,
            f"slow path should match metadata ({meta_second}) after second checkpoint")

    # Test block_size is correct immediately after restart, before any new checkpoint is taken.
    def test_block_size_after_restart(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(1000)
        self.session.checkpoint()

        size_before = self.get_block_size_fast()

        with self.expectedStdoutPattern("Removing local file"):
            self.reopen_conn()

        self.assertEqual(self.get_block_size_fast(), size_before,
            f"fast-path block_size should match the pre-restart checkpoint size")
        self.assertEqual(self.get_block_size_slow(), size_before,
            f"slow-path block_size should be restored from metadata after restart")

        # A new checkpoint after restart should keep both paths in sync.
        self.insert_rows(500, start=1000)
        self.session.checkpoint()

        fast_new = self.get_block_size_fast()
        meta_new = self.get_ckpt_size_from_meta()

        self.assertGreater(fast_new, size_before,
            "fast-path block_size should increase after inserting data and checkpointing post-restart")
        self.assertEqual(fast_new, meta_new,
            f"fast path ({fast_new}) should match metadata ({meta_new}) after post-restart checkpoint")
        self.assertEqual(self.get_block_size_slow(), meta_new,
            f"slow path should match metadata ({meta_new}) after post-restart checkpoint")

    # statistics=(size) on a layered: URI aggregates both ingest and stable block_size via the
    # slow path (opens both dhandles). Verify the result is non-zero and larger than the
    # stable-only value, since the ingest table contributes too.
    def test_block_size_layered_uri_includes_ingest(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.insert_rows(500)
        self.session.checkpoint()

        stable_size = self.get_block_size_slow()

        cstat = self.session.open_cursor('statistics:' + self.uri, None, 'statistics=(all)')
        layered_size = cstat[stat.dsrc.block_size][2]
        cstat.close()

        self.assertGreater(layered_size, 0,
            "block_size for a layered URI should be non-zero after a checkpoint")
        self.assertGreaterEqual(layered_size, stable_size,
            f"layered block_size ({layered_size}) should be >= stable block_size ({stable_size})")

    # When a table is created with block_manager=disagg, it gets a table: metadata entry with
    # block_manager=disagg. statistics=(size) on that table URI exercises the schema_stat.c
    # disagg fast path, which looks up checkpoint size from the file's metadata entry rather
    # than reading a file from disk.
    def test_block_size_table_uri_with_disagg_block_manager(self):
        table_base = "test_disagg_ckpt_size05_bm"
        table_uri = "table:" + table_base
        file_uri = "file:" + table_base + ".wt"

        self.session.create(table_uri, 'key_format=S,value_format=S,block_manager=disagg')

        self.insert_rows(500, uri=table_uri)
        self.session.checkpoint()

        # Fast path via table: URI exercises schema_stat.c disagg branch.
        cstat = self.session.open_cursor('statistics:' + table_uri, None, 'statistics=(size)')
        fast_table = cstat[stat.dsrc.block_size][2]
        cstat.close()

        # Fast path via file: URI exercises cur_stat.c disagg branch (ground truth).
        cstat = self.session.open_cursor('statistics:' + file_uri, None, 'statistics=(size)')
        fast_file = cstat[stat.dsrc.block_size][2]
        cstat.close()

        self.assertGreater(fast_table, 0,
            "statistics=(size) on table: URI with block_manager=disagg should return non-zero block_size")
        self.assertEqual(fast_table, fast_file,
            f"table: fast path ({fast_table}) should match file: fast path ({fast_file})")
