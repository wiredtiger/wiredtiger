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

import compatibility_test, compatibility_version, os, wiredtiger

class test_disagg_shared_metadata_config(compatibility_test.CompatibilityTestCase):
    '''
    Test a node picking up disaggregated checkpoints written by an older release.

    Older releases recognized shared trees by their ".wt_stable" name, so their shared metadata
    entries may lack block_manager=disagg. The new node must still treat every entry it picks up
    as a shared tree.
    '''

    uri = 'layered:test_disagg_shared_metadata_config'
    stable_uri = 'file:test_disagg_shared_metadata_config.wt_stable'
    create_config = 'key_format=S,value_format=S'
    nitems = 5000

    old_home = 'node_old'
    new_home = 'node_new'
    meta_file = 'checkpoint_meta.txt'

    def disagg_conn_config(self, branch, role):
        ext_path = os.path.join(self.branch_build_path(branch.name),
                                'ext', 'page_log', 'palite', 'libwiredtiger_palite.so')
        return f'create,statistics=(all),extensions=["{ext_path}"],' + \
            f'disaggregated=(page_log=palite,lose_all_my_data=true,role="{role}")'

    def test_disagg_shared_metadata_config(self):
        # Disaggregated storage first shipped in mongodb-9.0.
        disagg_version = compatibility_version.WTVersion('mongodb-9.0')
        if self.older_branch < disagg_version:
            self.skipTest(f'{self.older_branch.name} does not support disaggregated storage')

        self.run_method_on_branch(self.older_branch, 'populate_on_older_branch')
        self.run_method_on_branch(self.newer_branch, 'pick_up_on_newer_branch')

    def populate_on_older_branch(self):
        '''
        The old leader persists rows under a running transaction and leaves its checkpoints
        behind. A role cycle in the middle starts a new run, so the final checkpoint records a
        run write generation well past the small generations of a freshly started node: the new
        leader below cannot recognize the checkpoint as cross-run from its own generations
        alone.
        '''
        os.mkdir(self.old_home)
        conn = wiredtiger.wiredtiger_open(self.old_home,
            self.disagg_conn_config(self.older_branch, 'leader'))
        session = conn.open_session()
        session.create(self.uri, self.create_config)

        # Populate under an open transaction so the populating transactions' ids are persisted
        # rather than cleared.
        pin_session = conn.open_session()
        pin_session.begin_transaction()

        cursor = session.open_cursor(self.uri)
        for i in range(1, self.nitems + 1):
            session.begin_transaction()
            cursor[str(i)] = 'v' * 20
            session.commit_transaction('commit_timestamp=' + f'{10:x}')
        cursor.close()
        conn.set_timestamp('stable_timestamp=' + f'{10:x}')
        session.checkpoint()

        pin_session.rollback_transaction()
        pin_session.close()

        # Step down and back up to start a new run, then checkpoint again.
        conn.reconfigure('disaggregated=(role="follower")')
        conn.reconfigure('disaggregated=(role="leader")')

        cursor = session.open_cursor(self.uri)
        for i in range(1, 100):
            session.begin_transaction()
            cursor[str(i)] = 'w' * 20
            session.commit_transaction('commit_timestamp=' + f'{20:x}')
        cursor.close()
        conn.set_timestamp('stable_timestamp=' + f'{20:x}')
        session.checkpoint()

        # Hand the last complete checkpoint's metadata to the new node.
        page_log = conn.get_page_log('palite')
        (_, _, _, meta) = page_log.pl_get_complete_checkpoint(session)
        page_log.terminate(session)
        with open(self.meta_file, 'w') as f:
            f.write(meta)

        session.close()
        conn.close()

    def local_entry(self, session, uri):
        cursor = session.open_cursor('file:WiredTiger.wt')
        cursor.set_key(uri)
        assert cursor.search() == 0, f'{uri} is missing from the local metadata'
        value = cursor.get_value()
        cursor.close()
        return value

    def pick_up_on_newer_branch(self):
        '''
        A new node picks up the old node's checkpoint and steps up. The picked up stable entry
        must be a shared tree and every row the old node committed must be visible.
        '''
        os.mkdir(self.new_home)
        # Share the page log data written by the old node.
        os.symlink(os.path.join('..', self.old_home, 'kv_home'),
                   os.path.join(self.new_home, 'kv_home'), target_is_directory=True)

        conn = wiredtiger.wiredtiger_open(self.new_home,
            self.disagg_conn_config(self.newer_branch, 'follower'))
        session = conn.open_session()
        session.create(self.uri, self.create_config)

        with open(self.meta_file, 'r') as f:
            meta = f.read()
        conn.reconfigure(f'disaggregated=(checkpoint_meta="{meta}",role="leader")')

        assert 'block_manager=disagg' in self.local_entry(session, self.stable_uri), \
            'the picked up stable entry is not a shared tree'

        cursor = session.open_cursor(self.uri)
        count = 0
        while cursor.next() == 0:
            count += 1
        cursor.close()
        assert count == self.nitems, f'expected {self.nitems} rows, got {count}'

        # The new leader's own checkpoint completes over the inherited data.
        session.checkpoint()

        session.close()
        conn.close()

if __name__ == '__main__':
    compatibility_test.run()
