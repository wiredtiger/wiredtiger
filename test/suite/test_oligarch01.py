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

import os, wiredtiger, wttest
from helper_tiered import TieredConfigMixin, gen_tiered_storage_sources, get_shared_conn_config
from wtscenario import make_scenarios

StorageSource = wiredtiger.StorageSource  # easy access to constants

# test_oligarch1.py
#    Basic oligarch tree creation test
class test_oligarch1(wttest.WiredTigerTestCase, TieredConfigMixin):

    uri_base = "test_oligarch1"
    conn_config = 'log=(enabled),verbose=[oligarch]'

    uri = "oligarch:" + uri_base

    metadata_uris = [
            (uri, ''),
            ("file:" + uri_base + ".wt_ingest", ''),
            ("file:" + uri_base + ".wt_stable", '')
            ]

    # Load the directory store extension, which has object storage support
    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('storage_sources', 'dir_store')

    # Check for a specific string as part of the uri's metadata.
    def check_metadata(self, uri, val_str):
        c = self.session.open_cursor('metadata:create')
        val = c[uri]
        c.close()
        self.assertTrue(val_str in val)

    # Test calling the create API for an oligarch table.
    def test_oligarch1(self):
        base_create = 'key_format=S,value_format=S,storage_source=dir_store'

        self.pr("create oligarch tree")
        #conf = ',oligarch=true'
        conf = ''
        self.session.create(self.uri, base_create + conf)

        for u in self.metadata_uris:
            #print("Checking " + u[0])
            self.check_metadata(u[0], u[1])

