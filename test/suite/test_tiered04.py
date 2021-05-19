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
from wiredtiger import stat
StorageSource = wiredtiger.StorageSource  # easy access to constants

# test_tiered04.py
#    Basic tiered storage API test.
class test_tiered04(wttest.WiredTigerTestCase):

    # If the 'uri' changes all the other names must change with it.
    fileuri = 'file:test_tiered04-0000000001.wtobj'
    objuri = 'object:test_tiered04-0000000001.wtobj'
    tiereduri = "tiered:test_tiered04"
    uri = "table:test_tiered04"

    uri1 = "table:test_other_tiered04"
    uri_none = "table:test_local04"

    auth_token = "test_token"
    bucket = "mybucket"
    bucket1 = "otherbucket"
    extension_name = "local_store"
    prefix = "this_pfx"
    prefix1 = "other_pfx"
    object_sys = "9M"
    object_sys_val = 9 * 1024 * 1024
    object_uri = "15M"
    object_uri_val = 15 * 1024 * 1024
    retention = 600
    retention1 = 350
    def conn_config(self):
        os.mkdir(self.bucket)
        os.mkdir(self.bucket1)
        return \
          'statistics=(all),' + \
          'tiered_storage=(auth_token=%s,' % self.auth_token + \
          'bucket=%s,' % self.bucket + \
          'bucket_prefix=%s,' % self.prefix + \
          'local_retention=%d,' % self.retention + \
          'name=%s,' % self.extension_name + \
          'object_target_size=%s)' % self.object_sys

    # Load the local store extension, but skip the test if it is missing.
    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        extlist.extension('storage_sources', self.extension_name)

    # Check for a specific string as part of the uri's metadata.
    def check_metadata(self, uri, val_str):
        c = self.session.open_cursor('metadata:')
        val = c[uri]
        c.close()
        self.assertTrue(val_str in val)

    def get_stat(self, stat, uri):
        if uri == None:
            stat_cursor = self.session.open_cursor('statistics:')
        else:
            stat_cursor = self.session.open_cursor('statistics:' + uri)
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    # Test calling the flush_tier API.
    def test_tiered(self):
        # Create three tables. One using the system tiered storage, one
        # specifying its own bucket and object size and one using no
        # tiered storage. Use stats to verify correct setup.
        intl_page = 'internal_page_max=16K'
        base_create = 'key_format=S,' + intl_page
        self.pr("create sys")
        self.session.create(self.uri, base_create)
        conf = \
          ',tiered_storage=(auth_token=%s,' % self.auth_token + \
          'bucket=%s,' % self.bucket1 + \
          'bucket_prefix=%s,' % self.prefix1 + \
          'local_retention=%d,' % self.retention1 + \
          'name=%s,' % self.extension_name + \
          'object_target_size=%s)' % self.object_uri
        self.pr("create non-sys tiered")
        self.session.create(self.uri1, base_create + conf)
        conf = ',tiered_storage=(name=none)'
        self.pr("create non tiered/local")
        self.session.create(self.uri_none, base_create + conf)

        #self.pr("open cursor")
        #c = self.session.open_cursor(self.uri)
        self.pr("flush tier")
        self.session.flush_tier(None)

        self.pr("flush tier again")
        self.session.flush_tier(None)
        calls = self.get_stat(stat.conn.flush_tier, None)
        self.assertEqual(calls, 2)
        obj = self.get_stat(stat.conn.tiered_object_size, None)
        self.assertEqual(obj, self.object_sys_val)

        self.check_metadata(self.tiereduri, intl_page)
        self.check_metadata(self.fileuri, intl_page)
        self.check_metadata(self.objuri, intl_page)

        #self.pr("verify stats")
        # Verify the table settings.
        #obj = self.get_stat(stat.dsrc.tiered_object_size, self.uri)
        #self.assertEqual(obj, self.object_sys_val)
        #obj = self.get_stat(stat.dsrc.tiered_object_size, self.uri1)
        #self.assertEqual(obj, self.object_uri_val)
        #obj = self.get_stat(stat.dsrc.tiered_object_size, self.uri_none)
        #self.assertEqual(obj, 0)

        #retain = self.get_stat(stat.dsrc.tiered_retention, self.uri)
        #self.assertEqual(retain, self.retention)
        #retain = self.get_stat(stat.dsrc.tiered_retention, self.uri1)
        #self.assertEqual(retain, self.retention1)
        #retain = self.get_stat(stat.dsrc.tiered_retention, self.uri_none)
        #self.assertEqual(retain, 0)

        # Now test some connection statistics with operations.
        retain = self.get_stat(stat.conn.tiered_retention, None)
        self.assertEqual(retain, self.retention)
        self.session.flush_tier(None)
        self.session.flush_tier('force=true')
        calls = self.get_stat(stat.conn.flush_tier, None)
        self.assertEqual(calls, 4)

        # Test reconfiguration.
        new = self.retention * 2
        config = 'tiered_storage=(local_retention=%d)' % new
        self.pr("reconfigure")
        self.conn.reconfigure(config)
        self.session.flush_tier(None)
        retain = self.get_stat(stat.conn.tiered_retention, None)
        calls = self.get_stat(stat.conn.flush_tier, None)
        self.assertEqual(retain, new)
        self.assertEqual(calls, 5)

if __name__ == '__main__':
    wttest.run()
