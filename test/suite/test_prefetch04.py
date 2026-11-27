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
# In jurisdictions that recognize copyright laws, the author or authors of
# this software dedicate any and all copyright interest in the software
# to the public domain. We make this dedication for the benefit of the
# public at large and to the detriment of our heirs and successors. We
# intend this dedication to be an overt act of relinquishment in
# perpetuity, subject to all future rights that could be granted to
# copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import wiredtiger, wttest
from itertools import product
from helper import WiredTigerCursor, statistic_uri

# Test the cursor-level prefetch=(enabled) setting.
#
# Keep the connection default off: with default=true, internal sessions also pre-fetch and bump
# the connection-wide counters, breaking the == 0 checks.

ENABLED, DISABLED = 'prefetch=(enabled=true)', 'prefetch=(enabled=false)'

class test_prefetch04(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),cache_size=2GB,prefetch=(available=true,default=false)'

    def stat(self, key):
        with WiredTigerCursor(self.session, statistic_uri()) as c:
            return c[key][2]

    def populate(self, uri, colgroups=False):
        cfg = 'allocation_size=512,leaf_page_max=512,key_format=i,'
        if colgroups:
            self.session.create(uri, cfg + 'value_format=ii,columns=(k,v1,v2),colgroups=(c1,c2)')
            name = uri.split(':')[1]
            self.session.create(f'colgroup:{name}:c1', 'columns=(v1)')
            self.session.create(f'colgroup:{name}:c2', 'columns=(v2)')
        else:
            self.session.create(uri, cfg + 'value_format=i')
        c = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 10001):
            c[i] = (1, 2) if colgroups else 1
        self.session.commit_transaction()
        c.close()
        self.session.checkpoint()

    # Scan the whole table, returning the change in pre-fetch attempts and pages queued.
    def scan(self, session, uri, cursor_cfg, backward=False):
        attempts_before = self.stat(wiredtiger.stat.conn.prefetch_attempts)
        queued_before = self.stat(wiredtiger.stat.conn.prefetch_pages_queued)

        c = session.open_cursor(uri, None, cursor_cfg)
        step = c.prev if backward else c.next
        while step() == 0:
            pass
        c.close()

        return (self.stat(wiredtiger.stat.conn.prefetch_attempts) - attempts_before,
            self.stat(wiredtiger.stat.conn.prefetch_pages_queued) - queued_before)

    # The cursor's setting wins over the session's; an unset cursor inherits the session's.
    def test_precedence(self):
        uri = 'file:precedence'
        self.populate(uri)
        # Every combination of session setting, cursor setting and scan direction.
        for session_on, cursor_cfg, backward in product(
                (False, True), (None, ENABLED, DISABLED), (False, True)):
            # Reopen for a cold cache, so the scan reads from disk and can queue pages.
            self.reopen_conn('.', self.conn_config)
            session = self.conn.open_session(ENABLED if session_on else None)
            attempts, queued = self.scan(session, uri, cursor_cfg, backward)
            session.close()

            expect = session_on if cursor_cfg is None else cursor_cfg == ENABLED
            msg = f'session_on={session_on}, cursor={cursor_cfg}, backward={backward}'
            if expect:
                self.assertGreater(queued, 0, msg)
            else:
                self.assertEqual(attempts, 0, msg)

    # A cached cursor re-parses its setting on reuse. Each step reuses the cursor the previous
    # step cached (populate caches the first), so a leaked enabled setting fails step 2 and a
    # leaked disabled setting fails step 4.
    def test_reuse(self):
        # (session on, cursor config, expect pre-fetch)
        steps = [(False, ENABLED, True), (False, None, False),
                 (True, DISABLED, False), (True, None, True)]
        tables = [
            ('file:reuse', False),
            ('table:reuse', False),
            ('table:reuse_cg', True),  # Column groups: never cached, so every open is fresh.
        ]
        for uri, colgroups in tables:
            self.populate(uri, colgroups)
            for session_on, cursor_cfg, expect in steps:
                self.session.reconfigure(ENABLED if session_on else DISABLED)
                reopens = self.stat(wiredtiger.stat.conn.cursor_reopen)
                attempts, _ = self.scan(self.session, uri, cursor_cfg)

                msg = f'{uri}: session_on={session_on}, cursor={cursor_cfg}'
                if not colgroups:
                    self.assertGreater(self.stat(wiredtiger.stat.conn.cursor_reopen), reopens,
                        f'{msg}: cursor not reused from the cache')
                if expect:
                    self.assertGreater(attempts, 0, msg)
                else:
                    self.assertEqual(attempts, 0, msg)

    def assert_einval(self, fn, level):
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError, fn,
            f'/pre-fetching cannot be enabled for the {level}/')

    # Enabling pre-fetch fails when it is unavailable. The in-memory case also guards the
    # conn_prefetch.c fix, which stops the prefetch server startup turning pre-fetching back on
    # for in-memory databases.
    def test_unavailable(self):
        uri = 'file:unavailable'
        for conn_cfg in ('prefetch=(available=false,default=false)',
                         'in_memory=true,prefetch=(available=true,default=true)'):
            self.reopen_conn('.', conn_cfg)
            self.assert_einval(lambda: self.conn.open_session(ENABLED), 'session')
            self.session.create(uri, 'key_format=i')

            self.assert_einval(lambda: self.session.open_cursor(uri, None, ENABLED), 'cursor')  # Fresh open.
            self.session.open_cursor(uri, None, DISABLED).close()   # Always allowed.
            self.session.open_cursor(uri).close()                   # Caches the cursor.
            self.assert_einval(lambda: self.session.open_cursor(uri, None, ENABLED), 'cursor')  # Cache reuse.

            # The cached cursor must still be usable, not leaked or half-initialised.
            c = self.session.open_cursor(uri)
            self.assertEqual(c.next(), wiredtiger.WT_NOTFOUND)
            c.close()
