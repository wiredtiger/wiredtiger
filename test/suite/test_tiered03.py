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

import os, re
import wiredtiger, wtscenario, wttest
from wtdataset import SimpleDataSet

# test_tiered03.py
#    Test block-log-structured tree configuration options.
class test_tiered03(wttest.WiredTigerTestCase):
    K = 1024
    M = 1024 * K
    G = 1024 * M
    # TODO: tiered: change this to a table: URI, otherwise we are
    # not using tiered files.  The use of a second directory for
    # sharing would probably need to be reworked.
    uri = 'file:test_tiered03'

    # Occasionally add a lot of records, so that merges (and bloom) happen.
    record_count_scenarios = wtscenario.quick_scenarios(
        'nrecs', [10, 10000], [0.9, 0.1])

    scenarios = wtscenario.make_scenarios(record_count_scenarios, prune=100, prunelong=500)

    auth_token = "test_token"
    bucket = "mybucket"
    absolute_bucket_dir = None  # initialied in conn_config to an absolute path
    cache_dir = "mybucket-cache" # a relative pathname, it will not be shared
    bucket_prefix = "pfx_"
    extension_name = "local_store"

    def conn_config(self):
        # We have multiple connections that want to share a bucket directory.
        # The first time this function is called, we'll establish the absolute
        # path for the bucket, and always use that for the bucket name.
        # The cache directory name is a relative one, so it won't be shared
        # between connections.
        if self.absolute_bucket_dir == None:
            self.absolute_bucket_dir = os.path.join(os.getcwd(), self.bucket)
            os.mkdir(self.absolute_bucket_dir)
        return \
          'tiered_storage=(auth_token=%s,' % self.auth_token + \
          'bucket=%s,' % self.absolute_bucket_dir + \
          'cache_directory=%s,' % self.cache_dir + \
          'bucket_prefix=%s,' % self.bucket_prefix + \
          'name=%s)' % self.extension_name

    # Load the local store extension.
    def conn_extensions(self, extlist):
        # Windows doesn't support dynamically loaded extension libraries.
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('storage_sources', self.extension_name)

    # Test sharing data between a primary and a secondary
    def test_sharing(self):
        # FIXME: WT-8235 Enable the test once file containing transaction ids is supported.
        self.skipTest('Sharing the checkpoint file containing transaction ids is not supported')

        ds = SimpleDataSet(self, self.uri, 10)
        ds.populate()
        ds.check()
        self.session.checkpoint()
        ds.check()

        # Create a secondary database
        dir2 = os.path.join(self.home, 'SECONDARY')
        os.mkdir(dir2)
        conn2 = self.setUpConnectionOpen(dir2)
        session2 = conn2.open_session()

        # Reference the tree from the secondary:
        metac = self.session.open_cursor('metadata:')
        metac2 = session2.open_cursor('metadata:', None, 'readonly=0')
        uri2 = self.uri[:5] + '../' + self.uri[5:]
        metac2[uri2] = metac[self.uri] + ",readonly=1"

        cursor2 = session2.open_cursor(uri2)
        ds.check_cursor(cursor2)
        cursor2.close()

        newds = SimpleDataSet(self, self.uri, 10000)
        newds.populate()
        newds.check()
        self.session.checkpoint()
        newds.check()

        # Check we can still read from the last checkpoint
        cursor2 = session2.open_cursor(uri2)
        ds.check_cursor(cursor2)
        cursor2.close()

        # Bump to new checkpoint
        origmeta = metac[self.uri]
        checkpoint = re.search(r',checkpoint=\(.+?\)\)', origmeta).group(0)[1:]
        self.pr('Orig checkpoint: ' + checkpoint)
        session2.alter(uri2, checkpoint)
        self.pr('New metadata on secondaery: ' + metac2[uri2])

        # Check that we can see the new data
        cursor2 = session2.open_cursor(uri2)
        newds.check_cursor(cursor2)

if __name__ == '__main__':
    wttest.run()
