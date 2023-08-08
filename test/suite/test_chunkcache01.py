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

import os, wttest
from wtdataset import SimpleDataSet

class test_chunkcache01(wttest.WiredTigerTestCase):
    uri = 'table:test_chunkcache01'

    def conn_config(self):
        if not os.path.exists('bucket1'):
            os.mkdir('bucket1')
        return 'tiered_storage=(auth_token=Secret,bucket=bucket1,bucket_prefix=pfx_,name=dir_store),chunk_cache=[enabled=true,chunk_size=100MB,capacity=25GB,type=DRAM],'

    def conn_extensions(self, extlist):
        extlist.extension('storage_sources', 'dir_store')

    def test_chunkcache(self):
        ds = SimpleDataSet(self, self.uri, 10, config='key_format=S')
        ds.populate()
        ds.check()
        self.session.checkpoint()
        self.session.checkpoint('flush_tier=(enabled)')
        ds.check()

        self.close_conn()
        self.reopen_conn()
        ds = SimpleDataSet(self, self.uri, 10, config='key_format=S')
        ds.check()
