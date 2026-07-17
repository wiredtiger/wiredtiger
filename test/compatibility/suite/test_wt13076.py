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
# of this software dedicate any and all copyright interest in this
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

import compatibility_test
import wiredtiger
from wtscenario import make_scenarios


class test_wt13076(compatibility_test.CompatibilityTestCase):
    """Test timestamp aggregate compatibility across WT-9.0 and develop."""

    older = 'mongodb-9.0'
    newer = 'develop'
    scenarios = make_scenarios([
        ('partial', dict(delete_all=False)),
        ('full', dict(delete_all=True)),
    ])
    uri = 'table:wt13076'
    nrows = 100
    table_config = 'key_format=i,value_format=S,allocation_size=512,leaf_page_max=512,internal_page_max=512'

    def _create_deleted_pages(self):
        conn = wiredtiger.wiredtiger_open('.', 'create,statistics=(all)')
        session = conn.open_session()
        session.create(self.uri, self.table_config)
        cursor = session.open_cursor(self.uri)

        for key in range(self.nrows):
            session.begin_transaction()
            cursor[key] = 'value'
            session.commit_transaction('commit_timestamp=10')
        session.checkpoint()

        deleted = range(self.nrows) if self.delete_all else range(0, self.nrows, 2)
        for key in deleted:
            session.begin_transaction()
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)
            session.commit_transaction('commit_timestamp=20')
        session.checkpoint()
        cursor.close()
        session.close()
        conn.close()

    def _verify_pages(self, rounds):
        conn = wiredtiger.wiredtiger_open('.', 'statistics=(all)')
        session = conn.open_session()
        cursor = session.open_cursor(self.uri)
        deleted = set(range(self.nrows) if self.delete_all else range(0, self.nrows, 2))
        for round_number in range(rounds):
            start = self.nrows + round_number * 20
            deleted.update(
                range(start, start + 20)
                if self.delete_all
                else range(start, start + 20, 2)
            )
        for key in range(self.nrows + rounds * 20):
            cursor.set_key(key)
            ret = cursor.search()
            if key in deleted:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
            else:
                self.assertEqual(ret, 0)
                self.assertEqual(cursor.get_value(), 'value')
        cursor.close()
        session.checkpoint()
        session.close()
        conn.close()

    def _mutate_pages(self, round_number):
        conn = wiredtiger.wiredtiger_open('.', 'statistics=(all)')
        session = conn.open_session()
        cursor = session.open_cursor(self.uri)
        start = self.nrows + round_number * 20

        for key in range(start, start + 20):
            session.begin_transaction()
            cursor[key] = 'value'
            session.commit_transaction('commit_timestamp=30')

        deleted = range(start, start + 20) if self.delete_all else range(start, start + 20, 2)
        for key in deleted:
            session.begin_transaction()
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)
            session.commit_transaction('commit_timestamp=40')

        session.checkpoint()
        cursor.close()
        session.close()
        conn.close()

    def test_upgrade_9_0_to_develop(self):
        self.run_method_on_branch(self.older_branch, 'on_older_branch_create')
        self.run_method_on_branch(self.newer_branch, 'on_newer_branch_verify_and_mutate')
        self.run_method_on_branch(self.older_branch, 'on_older_branch_verify_and_mutate')
        self.run_method_on_branch(self.newer_branch, 'on_newer_branch_final_verify')

    def test_downgrade_develop_to_9_0(self):
        self.run_method_on_branch(self.newer_branch, 'on_newer_branch_create')
        self.run_method_on_branch(self.older_branch, 'on_older_branch_verify_and_mutate')
        self.run_method_on_branch(self.newer_branch, 'on_newer_branch_verify_and_mutate')
        self.run_method_on_branch(self.older_branch, 'on_older_branch_final_verify')

    def on_older_branch_create(self):
        self._create_deleted_pages()

    def on_newer_branch_create(self):
        self._create_deleted_pages()

    def on_older_branch_verify_and_mutate(self):
        self._verify_pages(1)
        self._mutate_pages(1)

    def on_newer_branch_verify_and_mutate(self):
        self._verify_pages(0)
        self._mutate_pages(0)

    def on_newer_branch_final_verify(self):
        self._verify_pages(2)

    def on_older_branch_final_verify(self):
        self._verify_pages(2)


if __name__ == '__main__':
    compatibility_test.run()
