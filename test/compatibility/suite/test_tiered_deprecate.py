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
    Test leftover tiered storage configuration across a database upgrade.

    A database created on a branch that still has tiered storage may persist
    tiered_storage settings in WiredTiger.basecfg. When that database is later
    opened on a branch where the feature is removed:
      - If tiered_storage.name is none, the open should succeed and existing
        tables should be readable (the keys are kept as parse stubs).
      - If tiered_storage.name is a real source, the open should fail with
        ENOTSUP.

    This is leftover connection configuration, not a leftover tiered: table.
    An enabled name fails at wiredtiger_open, so those tables are never opened.
    '''

    build_config = {'standalone': 'true'}
    conn_config = ''
    create_config = 'key_format=i,value_format=S'
    uri = 'table:test_tiered_deprecate'
    nrows = 100

    # Non-default local_retention so the subgroup is written to the basecfg.
    # name=none avoids loading a storage source on the older branch.
    older_create_config = 'tiered_storage=(name=none,local_retention=1)'

    def test_tiered_deprecate(self):

        # Removal currently lives on develop (and this branch once merged).
        # Update the boundary if it is backported to a release branch.
        removed_version = compatibility_version.WTVersion("develop")

        if self.older_branch >= removed_version:
            self.run_method_on_branch(self.newer_branch, 'tiered_enabled_unsupported')
            return

        if self.older_branch < removed_version and self.newer_branch >= removed_version:
            self.run_method_on_branch(self.older_branch, 'on_older_branch')
            self.run_method_on_branch(self.newer_branch, 'on_newer_branch_name_none')
            self._set_basecfg_tiered_name('dir_store')
            self.run_method_on_branch(self.newer_branch, 'on_newer_branch_name_enabled')

    def _set_basecfg_tiered_name(self, name):
        basecfg_path = 'WiredTiger.basecfg'
        with open(basecfg_path, 'r') as f:
            contents = f.read()
        assert 'tiered_storage=(name=none' in contents, \
            f'tiered_storage=(name=none not found in {basecfg_path}:\n{contents}'
        with open(basecfg_path, 'w') as f:
            f.write(contents.replace('tiered_storage=(name=none',
              'tiered_storage=(name=' + name, 1))

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
        conn = wiredtiger.wiredtiger_open('.', 'create,' + self.older_create_config)
        session = conn.open_session()

        session.create(self.uri, self.create_config)

        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            c[i] = str(i)
        c.close()
        session.close()
        conn.close()

    def on_newer_branch_name_none(self):
        conn = wiredtiger.wiredtiger_open('.', self.conn_config)
        session = conn.open_session()

        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            assert c[i] == str(i), f'data mismatch on key {i}'
        c.close()
        session.close()
        conn.close()

    def on_newer_branch_name_enabled(self):
        if not self._newer_rejects_enabled_tiered():
            return
        try:
            wiredtiger.wiredtiger_open('.', self.conn_config)
            assert False, 'wiredtiger_open should fail when basecfg has an enabled tiered name'
        except wiredtiger.WiredTigerError as e:
            assert 'tiered storage is not supported' in str(e)


if __name__ == '__main__':
    compatibility_test.run()
