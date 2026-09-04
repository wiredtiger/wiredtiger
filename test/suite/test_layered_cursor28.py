#!/usr/bin/env python3
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
#
# test_layered_cursor28.py
#
# A cursor operating on a just-removed key must behave the same on a layered table as on a plain
# table. The expected values in every check below are what the plain cursor reports.

import re
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

KEYS = [10, 20, 30, 40, 50]
K = 30
MISSING = 25
VAL = 'X'

# The observed effect of one op from the deleted slot, plus a following next() or prev().
class OpResult:
    def __init__(self):
        self.op_return_code = None      # the op's return code, or 'EINVAL'/'ENOTSUP'/'NOTFOUND'
        self.op_key = None              # key the cursor is left on after the op, None if unpositioned
        self.follow_return_code = None  # return code of the following next() or prev()
        self.follow_key = None          # key the following next() or prev() lands on

    def __repr__(self):
        return ('OpResult(op_return_code=%r, op_key=%r, follow_return_code=%r, follow_key=%r)'
                % (self.op_return_code, self.op_key, self.follow_return_code, self.follow_key))

def _seed_layered(t, uri):
    c = t.session.open_cursor(uri)
    for i, k in enumerate(KEYS, 1):
        t.session.begin_transaction()
        c[k] = 'orig%d' % k
        t.session.commit_transaction('commit_timestamp=' + t.timestamp_str(i))
    c.close()

# Open a transaction, position on K and remove it; return the cursor on the deleted slot.
def _deleted_cursor(t, sess, uri):
    sess.begin_transaction()
    c = sess.open_cursor(uri)
    c.set_key(K)
    t.assertEqual(c.search(), 0)
    t.assertEqual(c.remove(), 0)
    return c

# Each setup creates and populates the table once and returns the session to operate through.
def setup_plain(t):
    uri = 'table:test_layered_cursor28'
    t.session.create(uri, 'key_format=i,value_format=S')
    c = t.session.open_cursor(uri)
    for k in KEYS:
        c[k] = 'orig%d' % k
    c.close()
    return t.session, uri

def setup_leader(t):
    uri = 'layered:test_layered_cursor28'
    t.session.create(uri, 'key_format=i,value_format=S')
    _seed_layered(t, uri)
    return t.session, uri

def setup_follower(t):
    uri = 'layered:test_layered_cursor28'
    t.session.create(uri, 'key_format=i,value_format=S')
    # Leader writes land in the stable table; the follower's remove writes an ingest tombstone.
    _seed_layered(t, uri)
    t.session.checkpoint()
    t.conn_follow = t.wiredtiger_open('follower',
        t.extensionsConfig() + t.conn_base_config + 'disaggregated=(role="follower")')
    t.session_follow = t.conn_follow.open_session('')
    t.ignoreStdoutPattern('Picking up the same checkpoint again')
    t.disagg_advance_checkpoint(t.conn_follow)
    return t.session_follow, uri

_variants = [
    ('plain',    dict(setup=setup_plain)),
    ('leader',   dict(setup=setup_leader)),
    ('follower', dict(setup=setup_follower)),
]

@disagg_test_class
class test_layered_cursor28(wttest.WiredTigerTestCase):
    conn_base_config = ',create,cache_size=1GB,statistics=(all),'
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, _variants)

    conn_follow = None
    active = None
    uri = None

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def tearDown(self):
        if self.conn_follow is not None:
            self.conn_follow.close()
            self.conn_follow = None
        super().tearDown()

    def return_code(self, fn):
        try:
            r = fn()
        except wiredtiger.WiredTigerError as e:
            m = str(e)
            if wiredtiger.wiredtiger_strerror(wiredtiger.WT_NOTFOUND) in m:
                return 'NOTFOUND'
            if 'Invalid argument' in m:
                return 'EINVAL'
            if 'Operation not supported' in m:
                return 'ENOTSUP'
            return 'ERR:' + m.strip()
        return 'NOTFOUND' if r == wiredtiger.WT_NOTFOUND else r

    def get_key_safe(self, c):
        try:
            return c.get_key()
        except wiredtiger.WiredTigerError:
            return None

    def run_op(self, op, follow='next', unpositioned=False):
        # These ops legitimately log to stderr off a deleted or unpositioned slot.
        self.captureerr.setIgnorePattern(re.compile(
            'requires key be set|requires value be set'
            '|only permitted in a running transaction|not supported in implicit transactions'))

        # Every run works in a transaction that is rolled back, so one setup serves a whole test.
        if self.active is None:
            self.active, self.uri = self.setup(self)
        c = _deleted_cursor(self, self.active, self.uri)
        if unpositioned:
            c.reset()

        result = OpResult()
        result.op_return_code = self.return_code(lambda: op(c))
        result.op_key = self.get_key_safe(c)

        # A following next() or prev() shows whether an op that failed on the deleted key left the
        # cursor truly unpositioned.
        result.follow_return_code = self.return_code(lambda: getattr(c, follow)())
        result.follow_key = self.get_key_safe(c)

        self.active.rollback_transaction()
        c.close()
        return result

    # Assert every field separately, so a failure shows exactly which one diverged.
    def check(self, result, op_return_code, op_key, follow_return_code, follow_key):
        self.assertEqual(result.op_return_code, op_return_code, 'op_return_code')
        self.assertEqual(result.op_key, op_key, 'op_key')
        self.assertEqual(result.follow_return_code, follow_return_code, 'follow_return_code')
        self.assertEqual(result.follow_key, follow_key, 'follow_key')

    def test_get_key(self):
        result = self.run_op(lambda c: c.get_key())
        self.check(result, op_return_code=30, op_key=30, follow_return_code=0, follow_key=40)

    def test_get_value(self):
        result = self.run_op(lambda c: c.get_value())
        self.check(result, op_return_code='EINVAL', op_key=30, follow_return_code=0, follow_key=40)

    def test_next(self):
        result = self.run_op(lambda c: c.next())
        self.check(result, op_return_code=0, op_key=40, follow_return_code=0, follow_key=50)

    def test_prev(self):
        result = self.run_op(lambda c: c.prev())
        self.check(result, op_return_code=0, op_key=20, follow_return_code=0, follow_key=40)

    def test_search(self):
        result = self.run_op(lambda c: c.set_key(K) or c.search())
        self.check(result, op_return_code='NOTFOUND', op_key=30, follow_return_code=0, follow_key=10)

    def test_search_near(self):
        result = self.run_op(lambda c: c.set_key(K) or c.search_near())
        self.check(result, op_return_code=1, op_key=40, follow_return_code=0, follow_key=50)

    def test_reset(self):
        result = self.run_op(lambda c: c.reset())
        self.check(result, op_return_code=0, op_key=None, follow_return_code=0, follow_key=10)

    def test_largest_key(self):
        result = self.run_op(lambda c: c.largest_key())
        self.check(result, op_return_code=0, op_key=50, follow_return_code=0, follow_key=10)

    def test_update(self):
        result = self.run_op(lambda c: c.set_value(VAL) or c.update())
        self.check(result, op_return_code=0, op_key=30, follow_return_code=0, follow_key=40)

    def test_insert(self):
        result = self.run_op(lambda c: c.set_key(K) or c.set_value(VAL) or c.insert())
        self.check(result, op_return_code=0, op_key=None, follow_return_code=0, follow_key=10)

    def test_reserve(self):
        result = self.run_op(lambda c: c.reserve())
        self.check(result, op_return_code='NOTFOUND', op_key=30, follow_return_code=0, follow_key=10)

    def test_remove(self):
        result = self.run_op(lambda c: c.remove())
        self.check(result, op_return_code='NOTFOUND', op_key=None, follow_return_code=0, follow_key=10)
        result = self.run_op(lambda c: c.remove(), follow='prev')
        self.check(result, op_return_code='NOTFOUND', op_key=None, follow_return_code=0, follow_key=50)

    # Removing by key from an unpositioned cursor keeps the application key on failure.
    def test_remove_by_key_missing(self):
        op = lambda c: c.set_key(MISSING) or c.remove()
        result = self.run_op(op, unpositioned=True)
        self.check(result, op_return_code='NOTFOUND', op_key=MISSING, follow_return_code=0, follow_key=10)
        result = self.run_op(op, follow='prev', unpositioned=True)
        self.check(result, op_return_code='NOTFOUND', op_key=MISSING, follow_return_code=0, follow_key=50)

    def test_remove_by_key_removed(self):
        op = lambda c: c.set_key(K) or c.remove()
        result = self.run_op(op, unpositioned=True)
        self.check(result, op_return_code='NOTFOUND', op_key=K, follow_return_code=0, follow_key=10)
        result = self.run_op(op, follow='prev', unpositioned=True)
        self.check(result, op_return_code='NOTFOUND', op_key=K, follow_return_code=0, follow_key=50)

    def test_modify(self):
        result = self.run_op(lambda c: c.modify([wiredtiger.Modify(VAL, 0, 0)]))
        self.check(result, op_return_code='NOTFOUND', op_key=30, follow_return_code=0, follow_key=10)
