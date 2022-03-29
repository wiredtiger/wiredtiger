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

from helper_tiered import get_auth_token, get_bucket1_name
import os, wiredtiger, wttest
from wtscenario import make_scenarios

# Test session.create with the exclusive configuration.
class test_create_excl(wttest.WiredTigerTestCase):
    storage_sources = [
        ('dir_store', dict(auth_token = get_auth_token('dir_store'),
            bucket = get_bucket1_name('dir_store'),
            bucket_prefix = "pfx_",
            ss_name = 'dir_store')),
    ]

    scenarios = make_scenarios(storage_sources)

    def conn_config(self):
        if self.ss_name == 'dir_store' and not os.path.exists(self.bucket):
            os.mkdir(self.bucket)
        return \
          'debug_mode=(flush_checkpoint=true),' + \
          'tiered_storage=(auth_token=%s,' % self.auth_token + \
          'bucket=%s,' % self.bucket + \
          'bucket_prefix=%s,' % self.bucket_prefix + \
          'name=%s),tiered_manager=(wait=0)' % self.ss_name

    def conn_extensions(self, extlist):
        config = ''
        # S3 store is built as an optional loadable extension, not all test environments build S3.
        if self.ss_name == 's3_store':
            #config = '=(config=\"(verbose=1)\")'
            extlist.skip_if_missing = True
        #if self.ss_name == 'dir_store':
            #config = '=(config=\"(verbose=1,delay_ms=200,force_delay=3)\")'
        # Windows doesn't support dynamically loaded extension libraries.
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('storage_sources', self.ss_name + config)

    def test_create_excl(self):
        file_uri = "file:create_excl_file"
        table_uri = "table:create_excl_table"
        tiered_uri = "table:create_excl_tiered"

        self.session.create(file_uri, "exclusive=true")
        self.session.create(table_uri, "exclusive=true,tiered_storage=(name=none)")
        self.session.create(tiered_uri, "exclusive=true")

        # Exclusive re-create should error.
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.session.create(file_uri, "exclusive=true"))
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.session.create(table_uri, "exclusive=true,tiered_storage=(name=none)"))
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.session.create(tiered_uri, "exclusive=true"))

        # Non-exclusive re-create is allowed.
        self.session.create(file_uri, "exclusive=false")
        self.session.create(table_uri, "exclusive=false")
        self.session.create(tiered_uri, "exclusive=false")

        # Exclusive create on a table that does not exist should succeed.
        self.session.create(file_uri + "non_existent", "exclusive=true")
        self.session.create(table_uri + "non_existent", "exclusive=true,tiered_storage=(name=none)")
        self.session.create(tiered_uri + "non_existent", "exclusive=true")

        # Non-exclusive create is allowed.
        self.session.create(file_uri + "non_existent1", "exclusive=false")
        self.session.create(table_uri + "non_existent1", "exclusive=false,tiered_storage=(name=none)")
        self.session.create(tiered_uri + "non_existent1", "exclusive=false")

if __name__ == '__main__':
    wttest.run()
