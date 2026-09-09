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

import os, compatibility_test, wiredtiger


class test_tiered_deprecate(compatibility_test.CompatibilityTestCase):
    '''
    Cross-version coverage for tiered-storage removal, using this checkout as
    the newer binary.

    Plain upgrade is the MongoDB shape: older writes a normal table (legacy
    file_meta keys, no enabled storage source), newer opens with
    config_base=false and reads the data.

    Plain downgrade: this checkout creates a table without those keys; older
    fills defaults from file_meta, reads, alters, and checkpoints.

    Enabled leftover is a second scenario: older dir_store + flush_tier.
    Opening with basecfg fails on the leftover connection name. Opening with
    config_base=false ignores basecfg; the table URI then fails as unsupported.
    '''

    # 'this' is this checkout. Keep it off SUITE_RELEASE_BRANCHES so other
    # tests do not pair against it.
    older = ['mongodb-8.0', 'mongodb-9.0']
    newer = 'this'

    build_config = {'standalone': 'true'}
    create_config = 'key_format=i,value_format=S'
    uri = 'table:test_tiered_deprecate'
    file_uri = 'file:test_tiered_deprecate.wt'
    meta_file_uri = 'file:WiredTiger.wt'
    bucket = 'bucket1'
    bucket_prefix = 'pfx_'
    nrows = 100

    # config_base=false so leftover WiredTiger.basecfg is ignored; logging off
    # so log version is not in play.
    open_config = 'create,config_base=false,log=(enabled=false)'

    plain_upgrade_home = 'plain_upgrade'
    plain_downgrade_home = 'plain_downgrade'
    enabled_home = 'enabled'

    def test_plain_upgrade_downgrade(self):
        self.run_method_on_branch(self.older_branch, 'on_older_plain')
        self.run_method_on_branch(self.newer_branch, 'on_newer_plain_upgrade')
        self.run_method_on_branch(self.newer_branch, 'on_newer_plain_create')
        self.run_method_on_branch(self.older_branch, 'on_older_plain_downgrade')

    def test_enabled_leftover(self):
        self.run_method_on_branch(self.older_branch, 'on_older_enabled')
        self.run_method_on_branch(self.newer_branch, 'on_newer_enabled_basecfg')
        self.run_method_on_branch(self.newer_branch, 'on_newer_enabled_no_basecfg')

    def _dir_store_path(self):
        return os.path.join(self.branch_build_path(self.older_branch),
          'ext', 'storage_sources', 'dir_store', 'libwiredtiger_dir_store.so')

    def _write_rows(self, session):
        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            c[i] = str(i)
        c.close()

    def _check_rows(self, session):
        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            assert c[i] == str(i)
        c.close()

    def _file_meta(self, session, uri):
        meta = session.open_cursor('metadata:')
        value = meta[uri]
        meta.close()
        return value

    def _assert_keys_absent(self, session, uri):
        value = self._file_meta(session, uri)
        assert 'tiered_storage=' not in value, value
        assert 'tiered_object=' not in value, value

    def _assert_keys_present(self, session, uri):
        value = self._file_meta(session, uri)
        assert 'tiered_storage=' in value, value

    def on_older_plain(self):
        os.mkdir(self.plain_upgrade_home)
        conn = wiredtiger.wiredtiger_open(self.plain_upgrade_home, self.open_config)
        session = conn.open_session()
        session.create(self.uri, self.create_config)
        self._write_rows(session)
        session.checkpoint()
        self._assert_keys_present(session, self.file_uri)
        session.close()
        conn.close()

    def on_newer_plain_upgrade(self):
        conn = wiredtiger.wiredtiger_open(self.plain_upgrade_home, self.open_config)
        session = conn.open_session()
        self._check_rows(session)
        self._assert_keys_present(session, self.file_uri)
        session.close()
        conn.close()

    def on_newer_plain_create(self):
        os.mkdir(self.plain_downgrade_home)
        conn = wiredtiger.wiredtiger_open(self.plain_downgrade_home, self.open_config)
        session = conn.open_session()
        session.create(self.uri, self.create_config)
        self._write_rows(session)
        session.checkpoint()
        self._assert_keys_absent(session, self.file_uri)
        self._assert_keys_absent(session, self.meta_file_uri)
        session.close()
        conn.close()

    def on_older_plain_downgrade(self):
        conn = wiredtiger.wiredtiger_open(self.plain_downgrade_home, self.open_config)
        session = conn.open_session()
        self._check_rows(session)
        session.alter(self.uri, 'access_pattern_hint=random')
        session.checkpoint()
        session.close()
        conn.close()

        conn = wiredtiger.wiredtiger_open(self.plain_downgrade_home, self.open_config)
        session = conn.open_session()
        self._check_rows(session)
        session.close()
        conn.close()

    def on_older_enabled(self):
        ext = self._dir_store_path()
        assert os.path.exists(ext), f'dir_store extension not found: {ext}'
        os.mkdir(self.enabled_home)
        os.mkdir(os.path.join(self.enabled_home, self.bucket))

        conn_config = (
          'create,tiered_storage=(name=dir_store,bucket=%s,bucket_prefix=%s),'
          'extensions=(%s)' % (self.bucket, self.bucket_prefix, ext))
        conn = wiredtiger.wiredtiger_open(self.enabled_home, conn_config)
        session = conn.open_session()
        session.create(self.uri, self.create_config)
        self._write_rows(session)
        session.checkpoint('flush_tier=(enabled)')
        meta = session.open_cursor('metadata:')
        found = any(k.startswith('tiered:') for k, _v in meta)
        meta.close()
        assert found, 'older branch did not create a tiered: metadata entry'
        session.close()
        conn.close()

    def on_newer_enabled_basecfg(self):
        try:
            wiredtiger.wiredtiger_open(self.enabled_home, 'log=(enabled=false)')
            assert False, 'wiredtiger_open of a leftover enabled-tiered home should fail'
        except wiredtiger.WiredTigerError as e:
            assert 'tiered storage is not supported' in str(e)

    def on_newer_enabled_no_basecfg(self):
        conn = wiredtiger.wiredtiger_open(self.enabled_home, self.open_config)
        session = conn.open_session()
        try:
            session.open_cursor(self.uri)
            assert False, 'opening a leftover tiered table should fail'
        except wiredtiger.WiredTigerError as e:
            msg = str(e)
            assert 'unsupported object operation' in msg or 'unknown object type' in msg, msg
        session.close()
        conn.close()


if __name__ == '__main__':
    compatibility_test.run()
