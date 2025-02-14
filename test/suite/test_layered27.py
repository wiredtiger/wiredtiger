#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2027-2014 WiredTiger, Inc.
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

import time
import wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered27.py
# Simple read write testing using the page log API

@disagg_test_class
class test_layered27(wttest.WiredTigerTestCase, DisaggConfigMixin):
    encrypt = [
        ('none', dict(encryptor='none', encrypt_args='')),
        ('rotn', dict(encryptor='rotn', encrypt_args='keyid=13')),
    ]

    compress = [
        ('none', dict(block_compress='none')),
        ('snappy', dict(block_compress='snappy')),
    ]

    # A small cache size is important here - the workload only inserts a small amount of data
    # but it needs to put some pressure on cache utilization.
    # The verbose read flag is critical - we don't want this workload to trigger any reads at
    # the moment - some versions of WiredTiger were evicting metadata pages for this simple workload
    # and that isn't necessary (and detracts from overall system efficiency)
    conn_base_config = 'cache_size=20MB,transaction_sync=(enabled,method=fsync),' \
            + 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'disaggregated=(page_log=palm),verbose=[read],'
    disagg_storages = gen_disagg_storages('test_layered27', disagg_only = True)

    scenarios = make_scenarios(encrypt, compress, disagg_storages)

    nitems = 50000

    def conn_config(self):
        enc_conf = 'encryption=(name={0},{1})'.format(self.encryptor, self.encrypt_args)
        return self.conn_base_config + 'disaggregated=(role="leader"),' + enc_conf

    # Load the storage store extension.
    def conn_extensions(self, extlist):
        extlist.extension('compressors', self.block_compress)
        extlist.extension('encryptors', self.encryptor)
        DisaggConfigMixin.conn_extensions(self, extlist)

    def test_layered_read_write(self):
        uri = "layered:test_layered27"
        create_session_config = 'key_format=S,value_format=S,block_compressor={}'.format(self.block_compress)
        self.pr('CREATING')
        self.session.create(uri, create_session_config)

        cursor = self.session.open_cursor(uri, None, None)

        for i in range(self.nitems):
            cursor["Hello " + str(i)] = "World"
        cursor.close()

        time.sleep(1)
        self.session.checkpoint()
        time.sleep(1)

        cursor = self.session.open_cursor(uri, None, None)
        for i in range(self.nitems):
            cursor["Hello2 " + str(i)] = "World"
        cursor.close()
        time.sleep(1)

        self.session.checkpoint()
