#!/usr/bin/env python
#
# Public Domain 2014-2019 MongoDB, Inc.
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
import wiredtiger, wttest
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

# test_log04.py
#    test changing log compressors over a restart
class test_log04(wttest.WiredTigerTestCase):
    homedir = 'HOME'
    uri = 'table:test_log04'
    nentries = 20000

    init_compression_values = [
        ('none', dict(init_compress=None)),
        ('nop', dict(init_compress='nop')),
        ('lz4', dict(init_compress='lz4')),
        ('snappy', dict(init_compress='snappy')),
        ('zlib', dict(init_compress='zlib')),
        ('zstd', dict(init_compress='zstd'))
    ]
    after_compression_values = [
        ('none', dict(after_compress=None)),
        ('nop', dict(after_compress='nop')),
        ('lz4', dict(after_compress='lz4')),
        ('snappy', dict(after_compress='snappy')),
        ('zlib', dict(after_compress='zlib')),
        ('zstd', dict(after_compress='zstd'))
    ]
    scenarios = make_scenarios(init_compression_values, after_compression_values)

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        if self.init_compress is not None:
            extlist.extension('compressors', self.init_compress)
        if self.after_compress is not None:
            extlist.extension('compressors', self.after_compress)

    def populate(self):
        big_str = 'A' * 10000
        self.session.create(self.uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nentries):
            self.session.begin_transaction()
            cursor[str(i)] = big_str
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(2))
        cursor.close()

    def make_config_string(self, compression):
        config_str = 'create,log=(enabled'
        if compression is not None:
            config_str += ',compressor={0}'.format(compression)
        config_str += ')'
        return config_str

    def test_change_log_compression(self):
        os.mkdir(self.homedir)

        self.conn = self.wiredtiger_open(
            self.homedir, self.make_config_string(self.init_compress))
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(1))
        self.session = self.conn.open_session('isolation=snapshot')
        self.populate()
        self.session.close()
        self.conn.close()

        with self.expectedStderrPattern(""):
            self.conn = self.wiredtiger_open(
                self.homedir, self.make_config_string(self.after_compress))

if __name__ == '__main__':
    wttest.run()
