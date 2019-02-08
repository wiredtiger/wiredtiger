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
#
# test_encrypt01.py
#   Basic block encryption operations
#

import os, run, random
import wiredtiger, wttest
from wtscenario import make_scenarios

# Test basic encryption
class test_encrypt01(wttest.WiredTigerTestCase):

    types = [
        ('file', dict(uri='file:test_encrypt01')),
        ('table', dict(uri='table:test_encrypt01')),
    ]
    encrypt = [
        ('none', dict( sys_encrypt='none', sys_encrypt_args='',
            file_encrypt='none', file_encrypt_args='')),
        ('nop', dict( sys_encrypt='nop', sys_encrypt_args='',
            file_encrypt='nop', file_encrypt_args='')),
        ('rotn', dict( sys_encrypt='rotn', sys_encrypt_args=',keyid=11',
            file_encrypt='rotn', file_encrypt_args=',keyid=13')),
        ('rotn-none', dict( sys_encrypt='rotn', sys_encrypt_args=',keyid=9',
            file_encrypt='none',  file_encrypt_args=''))
    ]
    compress = [
        ('none', dict(log_compress=None, block_compress=None)),
        ('nop', dict(log_compress='nop', block_compress='nop')),
        ('lz4', dict(log_compress='lz4', block_compress='lz4')),
        ('snappy', dict(log_compress='snappy', block_compress='snappy')),
        ('zlib', dict(log_compress='zlib', block_compress='zlib')),
        ('zstd', dict(log_compress='zstd', block_compress='zstd')),
        ('none-snappy', dict(log_compress=None, block_compress='snappy')),
        ('snappy-lz4', dict(log_compress='snappy', block_compress='lz4')),
    ]
    scenarios = make_scenarios(types, encrypt, compress)

    nrecords = 5000
    bigvalue = "abcdefghij" * 1001    # len(bigvalue) = 10010

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        extlist.extension('encryptors', self.sys_encrypt)
        extlist.extension('encryptors', self.file_encrypt)
        extlist.extension('compressors', self.block_compress)
        extlist.extension('compressors', self.log_compress)

    def conn_config(self):
        encarg = 'encryption=(name={0}{1}),'.format(
            self.sys_encrypt, self.sys_encrypt_args)
        comparg = ''
        if self.log_compress != None:
            comparg='log=(compressor={0}),'.format(self.log_compress)
        return encarg + comparg

    # Create a table, add keys with both big and small values, then verify them.
    def test_encrypt(self):
        params = 'key_format=S,value_format=S'
        if self.file_encrypt != None:
            params += ',encryption=(name=' + self.file_encrypt + \
                      self.file_encrypt_args + ')'
        if self.block_compress != None:
            params += ',block_compressor=' + self.block_compress

        self.session.create(self.uri, params)
        cursor = self.session.open_cursor(self.uri, None)
        r = random.Random()
        r.seed(0)
        for idx in xrange(1,self.nrecords):
            start = r.randint(0,9)
            key = self.bigvalue[start:r.randint(0,100)] + str(idx)
            val = self.bigvalue[start:r.randint(0,10000)] + str(idx)
            cursor.set_key(key)
            cursor.set_value(val)
            cursor.insert()
        cursor.close()

        # Force the cache to disk, so we read
        # compressed/encrypted pages from disk.
        self.reopen_conn()

        cursor = self.session.open_cursor(self.uri, None)
        r.seed(0)
        for idx in xrange(1,self.nrecords):
            start = r.randint(0,9)
            key = self.bigvalue[start:r.randint(0,100)] + str(idx)
            val = self.bigvalue[start:r.randint(0,10000)] + str(idx)
            cursor.set_key(key)
            self.assertEqual(cursor.search(), 0)
            self.assertEquals(cursor.get_value(), val)
        cursor.close()

if __name__ == '__main__':
    wttest.run()
