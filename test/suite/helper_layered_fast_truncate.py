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

# helper_layered_fast_truncate.py
#   Shared helpers for the layered fast truncate Python tests.

from contextlib import closing
from itertools import chain
from typing import Iterable

import wiredtiger


def concat(*iterables):
    """Concatenate any number of iterables into a single list."""
    return list(chain.from_iterable(iterables))


def range_inclusive(start, stop):
    """Return a range covering [start, stop] inclusive."""
    return range(start, stop + 1)


class LayeredFastTruncateConfigMixin:
    """Shared helpers for the layered fast truncate test suite."""

    def setUp(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest("fast truncate support is not enabled")
        super().setUp()

    def _key(self, n):
        """Convert an int into the test's key format."""
        return n

    def session_create_config(self):
        """
        Return the create() configuration string. Defaults to int keys and
        string values; appends layered table options when uri starts with
        'table' (or 'table:').
        """
        cfg = 'key_format=i,value_format=S'
        uri = getattr(self, 'uri', '')
        if uri.startswith('table'):
            cfg += ',block_manager=disagg,type=layered'
        return cfg

    def auto_closing_cursor(self, config=None):
        """Return a cursor that auto-closes as it goes out of scope."""
        return closing(self.session.open_cursor(self.uri, None, config))

    def populate(self, keys, value='v'):
        """Insert each key with a placeholder value in a single transaction."""
        with self.auto_closing_cursor() as cursor:
            with self.transaction():
                for key in keys:
                    cursor[self._key(key)] = value

    def setup_leader(self, keys=None, extra_cfg=''):
        """
        Create the table on the leader and optionally populate stable. The
        follower picks up these keys via the initial checkpoint.
        """
        self.session.create(self.uri, self.session_create_config() + extra_cfg)
        if keys is not None:
            self.populate(keys)
        self.session.checkpoint()

    def setup_follower(self, keys=None):
        """Switch to follower role and optionally write keys to ingest."""
        self.reopen_disagg_conn('disaggregated=(role="follower"),')
        if keys is not None:
            self.populate(keys)

    def truncate(self, start_key=None, stop_key=None, commit_timestamp=None, session=None):
        """
        Truncate [start_key, stop_key] inclusive on self.uri. Either bound
        may be None for an open-ended side. If commit_timestamp is set,
        the truncate transaction commits at that timestamp. session
        defaults to self.session; pass an alternate session for tests
        that drive a separate follower connection.
        """
        sess = session or self.session
        start = stop = None
        try:
            if start_key is not None:
                start = sess.open_cursor(self.uri)
                start.set_key(self._key(start_key))
            if stop_key is not None:
                stop = sess.open_cursor(self.uri)
                stop.set_key(self._key(stop_key))
            # session.truncate() needs a URI iff both cursors are NULL.
            uri = self.uri if (start is None and stop is None) else None
            with self.transaction(session=sess, commit_timestamp=commit_timestamp):
                sess.truncate(uri, start, stop, None)
        finally:
            if start is not None:
                start.close()
            if stop is not None:
                stop.close()

    def visible_keys(self, forward=True):
        """Return all keys visible via a scan (forward or backward)."""
        result = []
        with self.auto_closing_cursor() as cursor:
            step = cursor.next if forward else cursor.prev
            with self.transaction(rollback=True):
                while step() == 0:
                    result.append(cursor.get_key())
        return result

    def key_exists(self, key):
        """Return True if key is visible to a search in its own transaction."""
        with self.auto_closing_cursor() as cursor:
            with self.transaction(rollback=True):
                cursor.set_key(self._key(key))
                return cursor.search() == 0

    def search_near_key(self, key):
        """
        Run search_near. Returns (exact, found_key). exact follows WT
        convention: 0 = exact, 1 = positioned above, -1 = positioned
        below, or WT_NOTFOUND if no visible keys exist (in which case
        found_key is None).
        """
        with self.auto_closing_cursor() as cursor:
            with self.transaction(rollback=True):
                cursor.set_key(self._key(key))
                exact = cursor.search_near()
                if exact == wiredtiger.WT_NOTFOUND:
                    return exact, None
                return exact, cursor.get_key()

    def leader_checkpoint(self, ts=None):
        """Set timestamps and checkpoint on the leader."""
        if ts is not None:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts) +
                                    ',oldest_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

    def step_up(self):
        """Promote self.conn_follow to leader; the original leader steps down."""
        self.ignoreStdoutPattern('Picking up the same checkpoint')
        self.disagg_switch_follower_and_leader(self.conn_follow)

    def open_follower(self):
        """
        Open a separate follower connection (distinct from setup_follower
        which reopens the existing connection). Returns (conn, sess).
        """
        conn = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() +
            ',create,cache_size=50MB,statistics=(all),disaggregated=(role="follower")')
        sess = conn.open_session('')
        sess.create(self.uri, 'key_format=i,value_format=S')
        self.disagg_advance_checkpoint(conn, self.conn)
        return conn, sess

    def search_at(self, sess, key, ts):
        """Search for key under a read_timestamp; return (ret, value)."""
        cur = sess.open_cursor(self.uri)
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        cur.set_key(key)
        ret = cur.search()
        val = cur.get_value() if ret == 0 else None
        sess.rollback_transaction()
        cur.close()
        return ret, val

    def evict_range(self, sess, start, stop, step=1):
        """Evict the page(s) backing keys [start, stop] on the given session."""
        evict_cur = sess.open_cursor(self.uri, None, 'debug=(release_evict)')
        sess.begin_transaction('read_timestamp=' + self.timestamp_str(10))
        for i in range(start, stop + 1, step):
            evict_cur.set_key(i)
            evict_cur.search()
            evict_cur.reset()
        evict_cur.close()
        sess.rollback_transaction()

    def get_stat(self, conn, stat_key):
        """Read a connection statistic on the given connection."""
        s = conn.open_session('')
        val = s.open_cursor('statistics:')[stat_key][2]
        s.close()
        return val

    # Step-up flow shared by tests 16, 17 (separate follower connection that
    # later gets promoted to leader).

    def populate_on_leader_timestamped(self, value='v', ts=10, set_oldest=False, n=None):
        """
        Per-key timestamped writes on the leader, then advance stable
        (and optionally oldest) and checkpoint. value may be a callable
        producing the value from the key index.
        """
        n = n if n is not None else self.nitems
        cursor = self.session.open_cursor(self.uri)
        for i in range(n):
            self.session.begin_transaction()
            cursor[i] = value(i) if callable(value) else value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()
        ts_cfg = 'stable_timestamp=' + self.timestamp_str(ts)
        if set_oldest:
            ts_cfg += ',oldest_timestamp=' + self.timestamp_str(1)
        self.conn.set_timestamp(ts_cfg)
        self.session.checkpoint()

    def setup_dual_conn_follower(self, table_config='key_format=i,value_format=S',
                                 statistics=False, value='v', ts=10, set_oldest=False):
        """
        Open a separate follower connection, create the table on both
        sides, populate the leader with per-key timestamped writes, and
        advance the follower's checkpoint. Sets self.conn_follow and
        self.session_follow.
        """
        conn_extra = ',statistics=(all)' if statistics else ''
        self.conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + ',create' + conn_extra +
            ',disaggregated=(role="follower")')
        self.session_follow = self.conn_follow.open_session('')
        self.session.create(self.uri, table_config)
        self.session_follow.create(self.uri, table_config)
        self.populate_on_leader_timestamped(value=value, ts=ts, set_oldest=set_oldest)
        self.disagg_advance_checkpoint(self.conn_follow)

    def write_kv(self, session, key, value, ts):
        """Write a single key/value pair on session at the given commit ts."""
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        cursor[key] = value
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def remove_kv(self, session, key, ts):
        """Remove a single key on session at the given commit ts."""
        cursor = session.open_cursor(self.uri)
        cursor.set_key(key)
        session.begin_transaction()
        cursor.remove()
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def assert_visible(self, session, keys, value=None, ts=None):
        """Open a read_timestamp transaction on session and assert each key is visible."""
        session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        cursor = session.open_cursor(self.uri)
        for k in keys:
            cursor.set_key(k)
            self.assertEqual(cursor.search(), 0, f"key {k} should be visible at ts={ts}")
            if value is not None:
                expected = value(k) if callable(value) else value
                self.assertEqual(cursor.get_value(), expected)
        cursor.close()
        session.rollback_transaction()

    def assert_deleted(self, session, keys, ts):
        """Open a read_timestamp transaction on session and assert each key is NOTFOUND."""
        session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        cursor = session.open_cursor(self.uri)
        for k in keys:
            cursor.set_key(k)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {k} should be deleted at ts={ts}")
        cursor.close()
        session.rollback_transaction()

    def assert_ranges_deleted(self, session, ranges, ts=None, n=None):
        """
        Sweep keys 0..n on session. Keys inside any (lo, hi) inclusive
        range must be NOTFOUND, others must be visible. If ts is set,
        the sweep runs at that read_timestamp.
        """
        n = n if n is not None else self.nitems
        if ts is not None:
            session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
        cursor = session.open_cursor(self.uri)
        for k in range(n):
            cursor.set_key(k)
            in_range = any(lo <= k <= hi for lo, hi in ranges)
            expected = wiredtiger.WT_NOTFOUND if in_range else 0
            self.assertEqual(cursor.search(), expected,
                f'key {k} {"should be deleted" if in_range else "should be visible"}'
                + (f' at ts={ts}' if ts is not None else ''))
        cursor.close()
        if ts is not None:
            session.rollback_transaction()
