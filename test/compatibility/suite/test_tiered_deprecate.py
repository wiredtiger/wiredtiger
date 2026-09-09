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
    Upgrade: a leftover enabled-tiered home from an older branch must fail
    wiredtiger_open on a branch where tiered storage is removed (connection
    name in WiredTiger.basecfg).

    Downgrade: a file created without persisted tiered_storage / tiered_object
    keys must still open on an older branch that lists those keys in file_meta.
    '''

    build_config = {'standalone': 'true'}
    conn_config = ''
    create_config = 'key_format=i,value_format=S'
    uri = 'table:test_tiered_deprecate'
    file_uri = 'file:test_tiered_deprecate.wt'
    bucket = 'bucket1'
    bucket_prefix = 'pfx_'
    nrows = 100
    upgrade_home = 'upgrade'
    downgrade_home = 'downgrade'

    def test_tiered_deprecate(self):

        # Removal currently lives on develop (and this branch once merged).
        # Change the boundary if a release branch also has the removal.
        removed_version = compatibility_version.WTVersion("develop")

        if self.older_branch >= removed_version:
            self.run_method_on_branch(self.newer_branch, 'tiered_enabled_unsupported')
            return

        if self.older_branch < removed_version and self.newer_branch >= removed_version:
            self.run_method_on_branch(self.older_branch, 'on_older_upgrade')
            self.run_method_on_branch(self.newer_branch, 'on_newer_upgrade')
            self.run_method_on_branch(self.newer_branch, 'on_newer_downgrade')
            self.run_method_on_branch(self.older_branch, 'on_older_downgrade')

    def _dir_store_path(self):
        return os.path.join(self.branch_build_path(self.older_branch),
          'ext', 'storage_sources', 'dir_store', 'libwiredtiger_dir_store.so')

    def _newer_rejects_enabled_tiered(self):
        '''
        True if this binary treats an enabled leftover name as unsupported.
        Skip the fail assertions when the newer binary still implements the
        feature (origin/develop until the removal lands).
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

    def _newer_omits_tiered_keys(self):
        '''
        True if this binary does not persist tiered_storage / tiered_object on
        a new file. Skip the omit asserts when the newer binary still writes
        the keys (origin/develop until that change lands).
        '''
        probe = 'probe_tiered_omit'
        os.mkdir(probe)
        try:
            conn = wiredtiger.wiredtiger_open(probe, 'create,log=(enabled=false)')
            session = conn.open_session()
            session.create('table:probe', 'key_format=i,value_format=S')
            meta = session.open_cursor('metadata:')
            value = meta['file:probe.wt']
            meta.close()
            session.close()
            conn.close()
            return 'tiered_storage=' not in value and 'tiered_object=' not in value
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

    def on_older_upgrade(self):
        ext = self._dir_store_path()
        assert os.path.exists(ext), f'dir_store extension not found: {ext}'
        os.mkdir(self.upgrade_home)
        os.mkdir(os.path.join(self.upgrade_home, self.bucket))

        conn_config = (
          'create,tiered_storage=(name=dir_store,bucket=%s,bucket_prefix=%s),'
          'extensions=(%s)' % (self.bucket, self.bucket_prefix, ext))
        conn = wiredtiger.wiredtiger_open(self.upgrade_home, conn_config)
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

    def on_newer_upgrade(self):
        if not self._newer_rejects_enabled_tiered():
            return
        try:
            wiredtiger.wiredtiger_open(self.upgrade_home, self.conn_config)
            assert False, 'wiredtiger_open of a leftover tiered database should fail'
        except wiredtiger.WiredTigerError as e:
            assert 'tiered storage is not supported' in str(e)

    def on_newer_downgrade(self):
        os.mkdir(self.downgrade_home)
        conn = wiredtiger.wiredtiger_open(
          self.downgrade_home, 'create,log=(enabled=false)')
        session = conn.open_session()
        session.create(self.uri, self.create_config)
        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            c[i] = str(i)
        c.close()
        session.checkpoint()

        if self._newer_omits_tiered_keys():
            meta = session.open_cursor('metadata:')
            value = meta[self.file_uri]
            meta.close()
            assert 'tiered_storage=' not in value, value
            assert 'tiered_object=' not in value, value

        session.close()
        conn.close()

    def on_older_downgrade(self):
        conn = wiredtiger.wiredtiger_open(
          self.downgrade_home, 'log=(enabled=false)')
        session = conn.open_session()
        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            assert c[i] == str(i)
        c.close()
        session.close()
        conn.close()


if __name__ == '__main__':
    compatibility_test.run()
