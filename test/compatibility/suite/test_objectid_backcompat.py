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

import compatibility_test, wiredtiger


class test_objectid_backcompat(compatibility_test.CompatibilityTestCase):
    """
    Cross-version on-disk contract for the block manager address cookie.

    Local files persist an object id in address and checkpoint cookies only when it is non-zero;
    ordinary file: tables carry object id 0, which is encoded as "omitted" (no trailing flag
    byte). After tiered storage removal (WT-18587), the local block manager still threads that
    object id through WT_BLOCK, the extent lists, and open/read/write. WT-18647 will remove that
    in-memory plumbing but must keep the cookie pack/unpack so existing databases (all data on
    object 0) keep loading unchanged. See test/catch2/block/unit/test_block_addr.cpp for the
    encoding of cookies with a non-zero object id, which no released build can create on disk.

    These tests exercise a plain upgrade and downgrade where every cookie is the object-0
    "omitted" form: the newer build must load the root and extent lists from checkpoint cookies
    written by the older build, read checksum-verified blocks, and checkpoint again. They guard
    the part of the contract that a real deployment upgrade touches.
    """

    older = ["mongodb-8.0", "mongodb-9.0"]
    newer = "develop"

    build_config = {"standalone": "true"}
    create_config = "key_format=i,value_format=S"
    uri = "table:test_objectid_backcompat"
    nrows = 100

    def _make_data(self, session):
        # Write rows, then overwrite half of them and checkpoint again so both the root and the
        # extent lists (alloc, avail and discard) are written to disk and re-read on load.
        session.create(self.uri, self.create_config)
        self._write_rows(session)
        session.checkpoint()
        c = session.open_cursor(self.uri)
        for i in range(2, self.nrows + 1, 2):
            c[i] = str(i) + "b"
        c.close()
        session.checkpoint()

    def _write_rows(self, session):
        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            c[i] = str(i)
        c.close()

    def _verify_data(self, session):
        c = session.open_cursor(self.uri)
        for i in range(1, self.nrows + 1):
            expected = str(i) + ("b" if i % 2 == 0 else "")
            assert c[i] == expected, f"data mismatch on key {i}"
        c.close()

    def _reopen_and_verify(self):
        conn = wiredtiger.wiredtiger_open(".", "")
        session = conn.open_session()
        self._verify_data(session)
        session.close()
        conn.close()

    def test_plain_upgrade(self):
        # Older writes the object-0 database; newer must open and rewrite it.
        self.run_method_on_branch(self.older_branch, "on_older_write")
        self.run_method_on_branch(self.newer_branch, "on_newer_upgrade")

    def test_plain_downgrade(self):
        # Newer writes the object-0 database; older must open it back.
        self.run_method_on_branch(self.newer_branch, "on_newer_write")
        self.run_method_on_branch(self.older_branch, "on_older_downgrade")

    def on_older_write(self):
        conn = wiredtiger.wiredtiger_open(".", "create")
        session = conn.open_session()
        self._make_data(session)
        session.close()
        conn.close()

    def on_newer_upgrade(self):
        conn = wiredtiger.wiredtiger_open(".", "")
        session = conn.open_session()
        self._verify_data(session)
        # A checkpoint reads the existing extent lists and rewrites the checkpoints.
        session.checkpoint()
        session.close()
        conn.close()
        # Reopen to confirm the upgraded database is still consistent.
        self._reopen_and_verify()

    def on_newer_write(self):
        conn = wiredtiger.wiredtiger_open(".", "create")
        session = conn.open_session()
        self._make_data(session)
        session.close()
        conn.close()

    def on_older_downgrade(self):
        conn = wiredtiger.wiredtiger_open(".", "")
        session = conn.open_session()
        self._verify_data(session)
        session.alter(self.uri, "access_pattern_hint=random")
        session.checkpoint()
        session.close()
        conn.close()
        self._reopen_and_verify()


if __name__ == "__main__":
    compatibility_test.run()
