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

import os, wiredtiger, wtscenario, wttest
from wtdataset import SimpleDataSet

# test_tiered02.py
#    Test tiered tree
class test_tiered02(wttest.WiredTigerTestCase):
    K = 1024
    M = 1024 * K
    G = 1024 * M
    uri = "table:test_tiered02"

    auth_token = "test_token"
    bucket = "mybucket"
    bucket_prefix = "pfx_"
    extension_name = "local_store"
    prefix = "pfx-"

    def conn_config(self):
        if not os.path.exists(self.bucket):
            os.mkdir(self.bucket)
        return \
          'statistics=(all),' + \
          'tiered_storage=(auth_token=%s,' % self.auth_token + \
          'bucket=%s,' % self.bucket + \
          'bucket_prefix=%s,' % self.prefix + \
          'name=%s),tiered_manager=(wait=0)' % self.extension_name

    # Load the local store extension, but skip the test if it is missing.
    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        extlist.extension('storage_sources', self.extension_name)

    def progress(self, s):
        self.verbose(3, s)
        self.pr(s)

    def confirm_flush(self, increase=True):
        got = sorted(list(os.listdir(self.bucket)))
        self.pr('Flushed objects: ' + str(got))
        if increase:
            self.assertGreater(len(got), self.flushed_objects)
        else:
            self.assertEqual(len(got), self.flushed_objects)
        self.flushed_objects = len(got)

    # Test tiered storage with the old prototype way of signaling flushing to the shared
    # tier via checkpoints.  When flush_tier is working, the checkpoint calls can be
    # replaced with flush_tier.
    def test_tiered(self):
        self.flushed_objects = 0
        args = 'key_format=S'

        intl_page = 'internal_page_max=16K'
        base_create = 'key_format=S,value_format=S,' + intl_page
        self.pr("create sys")
        #self.session.create(self.uri + 'xxx', base_create)

        self.progress('Create simple data set (10)')
        ds = SimpleDataSet(self, self.uri, 10, config=args)
        self.progress('populate')
        ds.populate()
        ds.check()
        self.progress('checkpoint')
        self.session.checkpoint()
        self.progress('flush_tier')
        self.session.flush_tier(None)
        self.confirm_flush()

        # FIXME-WT-7589 reopening a connection does not yet work.
        if False:
            self.close_conn()
            self.progress('reopen_conn')
            self.reopen_conn()
            # Check what was there before
            ds = SimpleDataSet(self, self.uri, 10, config=args)
            ds.check()

        self.progress('Create simple data set (50)')
        ds = SimpleDataSet(self, self.uri, 50, config=args)
        self.progress('populate')
        ds.populate()
        ds.check()
        self.progress('checkpoint')
        self.session.checkpoint()
        self.progress('flush_tier')
        self.session.flush_tier(None)
        self.confirm_flush()

        # FIXME-WT-7589 This test works up to this point, then runs into trouble.
        if True:
            return

        self.progress('Create simple data set (100)')
        ds = SimpleDataSet(self, self.uri, 100, config=args)
        self.progress('populate')
        ds.populate()
        ds.check()
        self.progress('checkpoint')
        self.session.checkpoint()
        self.progress('flush_tier')
        self.session.flush_tier(None)
        self.confirm_flush()

        self.progress('Create simple data set (200)')
        ds = SimpleDataSet(self, self.uri, 200, config=args)
        self.progress('populate')
        ds.populate()
        ds.check()
        self.progress('close_conn')
        self.close_conn()
        self.confirm_flush()  # closing the connection does a checkpoint

        self.progress('reopen_conn')
        self.reopen_conn()
        # Check what was there before
        ds = SimpleDataSet(self, self.uri, 200, config=args)
        ds.check()

        # Now add some more.
        self.progress('Create simple data set (300)')
        ds = SimpleDataSet(self, self.uri, 300, config=args)
        self.progress('populate')
        ds.populate()
        ds.check()

        # We haven't done a flush so there should be
        # nothing extra on the shared tier.
        self.confirm_flush(increase=False)
        self.progress('checkpoint')
        self.session.checkpoint()
        self.confirm_flush(increase=False)
        self.progress('END TEST')

if __name__ == '__main__':
    wttest.run()
