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

import wttest
from wiredtiger import stat
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_compress03.py
#   Verify the statistics related to compression are updated as expected.
#
class test_compress03(wttest.WiredTigerTestCase):

    compressors = [
        ('lz4', dict(compressors='lz4')),
        ('snappy', dict(compressors='snappy')),
        ('zlib', dict(compressors='zlib')),
        ('zstd', dict(compressors='zstd')),
    ]
    scenarios = make_scenarios(compressors)

    uri = "table:test_compress03"
    nrows = 100000
    valuea = "aaaaa"
    valueb = "bbbbb"

    def conn_config(self):
        return f'statistics=(fast),statistics_log=(json,on_close,wait=1,sources=(\"file:\"))'

    # Load the compression extension, skip the test if missing.
    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        extlist.extension('compressors', self.compressors)

    def verify_stats(self, uri):
        stat_cursor = self.session.open_cursor('statistics:' + uri, None, 'statistics=(fast)')
        total_reads = stat_cursor[stat.dsrc.compress_read][2]
        total_reads_hist = stat_cursor[stat.dsrc.compress_read_hist_ratio_2][2]
        total_reads_hist += stat_cursor[stat.dsrc.compress_read_hist_ratio_4][2]
        total_reads_hist += stat_cursor[stat.dsrc.compress_read_hist_ratio_8][2]
        total_reads_hist += stat_cursor[stat.dsrc.compress_read_hist_ratio_16][2]
        total_reads_hist += stat_cursor[stat.dsrc.compress_read_hist_ratio_32][2]
        total_reads_hist += stat_cursor[stat.dsrc.compress_read_hist_ratio_64][2]
        total_reads_hist += stat_cursor[stat.dsrc.compress_read_hist_ratio_max][2]
        # We expect at least a read of compressed data.
        assert total_reads and total_reads == total_reads_hist

        total_writes = stat_cursor[stat.dsrc.compress_write][2]
        total_writes_hist = stat_cursor[stat.dsrc.compress_write_hist_ratio_2][2]
        total_writes_hist += stat_cursor[stat.dsrc.compress_write_hist_ratio_4][2]
        total_writes_hist += stat_cursor[stat.dsrc.compress_write_hist_ratio_8][2]
        total_writes_hist += stat_cursor[stat.dsrc.compress_write_hist_ratio_16][2]
        total_writes_hist += stat_cursor[stat.dsrc.compress_write_hist_ratio_32][2]
        total_writes_hist += stat_cursor[stat.dsrc.compress_write_hist_ratio_64][2]
        total_writes_hist += stat_cursor[stat.dsrc.compress_write_hist_ratio_max][2]
        # We expect at least a write of compressed data.
        assert total_writes and total_writes == total_writes_hist

        stat_cursor.close()

    def large_updates(self, uri, value, ds, nrows, ts):
        cursor = self.session.open_cursor(uri)
        for i in range(0, nrows):
            self.session.begin_transaction()
            cursor[ds.key(i)] = value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def test_compress03(self):
        ds = SimpleDataSet(self, self.uri, 0, key_format="S", value_format="S", config=f"block_compressor={self.compressors}")
        ds.populate()

        self.large_updates(self.uri, self.valuea, ds, self.nrows, 1)
        self.session.checkpoint()
        self.large_updates(self.uri, self.valuea * 10, ds, self.nrows, 2)
        self.session.checkpoint()
        self.large_updates(self.uri, self.valuea * 100, ds, self.nrows, 3)
        self.session.checkpoint()
        self.large_updates(self.uri, self.valueb, ds, self.nrows, 4)
        self.session.checkpoint()
        self.large_updates(self.uri, self.valueb * 10, ds, self.nrows, 5)
        self.session.checkpoint()
        self.large_updates(self.uri, self.valueb * 100, ds, self.nrows, 6)
        self.session.checkpoint()
        self.verify_stats(self.uri)

if __name__ == '__main__':
    wttest.run()
