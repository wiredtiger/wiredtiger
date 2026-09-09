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

import os, shutil, compatibility_test, compatibility_version, wiredtiger


class test_tiered_deprecate(compatibility_test.CompatibilityTestCase):
    '''
    Create a real tiered table on an older branch, then open that database
    on a branch where tiered storage is removed.

    The leftover home has both an enabled connection name in WiredTiger.basecfg
    and tiered: metadata. wiredtiger_open should fail with ENOTSUP because of
    the leftover name. The metadata URIs are never opened.
    '''

    build_config = {'standalone': 'true'}
    conn_config = ''
    create_config = 'key_format=i,value_format=S'
    uri = 'table:test_tiered_deprecate'
    bucket = 'bucket1'
    bucket_prefix = 'pfx_'
    nrows = 100

    def test_tiered_deprecate(self):

        # Removal currently lives on develop (and this branch once merged).
        # Change the boundary if a release branch also has the removal.
        removed_version = compatibility_version.WTVersion("develop")

        if self.older_branch >= removed_version:
            self.run_method_on_branch(self.newer_branch, 'tiered_enabled_unsupported')
            return

        if self.older_branch < removed_version and self.newer_branch >= removed_version:
            self.run_method_on_branch(self.older_branch, 'on_older_branch')
            self.run_method_on_branch(self.newer_branch, 'on_newer_branch')

    def _dir_store_path(self):
        return os.path.join(self.branch_build_path(self.older_branch),
          'ext', 'storage_sources', 'dir_store', 'libwiredtiger_dir_store.so')

    def _newer_rejects_enabled_tiered(self):
        '''
        True if this binary treats an enabled leftover name as unsupported.
        Skip the fail assertions when the newer binary still implements the
        feature (origin/develop before the removal lands).
        '''
        probe = 'probe_tiered_removed'
        os.mkdir(probe)
        try:
            conn = wiredtiger.wiredtiger_open(probe, 'create,tiered_storage=(name=dir_store)')
            conn.close()
            return False
        except wiredtiger.WiredTigerError as e:
            return 'tiered storage is not supported' in str(e)
        finally:
            shutil.rmtree(probe, ignore_errors=True)

    def tiered_enabled_unsupported(self):
        if not self._newer_rejects_enabled_tiered():
            return
        try:
            wiredtiger.wiredtiger_open('.', 'create,tiered_storage=(name=dir_store)')
            assert False, 'wiredtiger_open with tiered_storage=(name=dir_store) should fail'
        except wiredtiger.WiredTigerError as e:
            assert 'tiered storage is not supported' in str(e)

    def on_older_branch(self):
        ext = self._dir_store_path()
        assert os.path.exists(ext), f'dir_store extension not found: {ext}'
        os.mkdir(self.bucket)

        conn_config = (
          'create,tiered_storage=(name=dir_store,bucket=%s,bucket_prefix=%s),'
          'extensions=(%s)' % (self.bucket, self.bucket_prefix, ext))
        conn = wiredtiger.wiredtiger_open('.', conn_config)
        session = conn.open_session()

        session.create(self.uri, self.create_config)
        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            c[i] = str(i)
        c.close()
        session.checkpoint('flush_tier=(enabled)')

        meta = session.open_cursor('metadata:')
        found = any(k.startswith('tiered:') for k, _v in meta)
        meta.close()
        assert found, 'older branch did not create a tiered: metadata entry'

        session.close()
        conn.close()

    def on_newer_branch(self):
        if not self._newer_rejects_enabled_tiered():
            return
        try:
            wiredtiger.wiredtiger_open('.', self.conn_config)
            assert False, 'wiredtiger_open of a leftover tiered database should fail'
        except wiredtiger.WiredTigerError as e:
            assert 'tiered storage is not supported' in str(e)


if __name__ == '__main__':
    compatibility_test.run()
