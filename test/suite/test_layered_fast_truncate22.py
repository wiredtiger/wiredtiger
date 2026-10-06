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

# Truncate at read-committed or read-uncommitted isolation is rejected on a
# follower.

from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_fast_truncate import LayeredFastTruncateConfigMixin, range_inclusive
from wiredtiger import WiredTigerError
from wtscenario import make_scenarios
import wttest


@disagg_test_class
class test_layered_fast_truncate22(LayeredFastTruncateConfigMixin, wttest.WiredTigerTestCase):
    """
    Truncate at read-committed or read-uncommitted isolation is rejected on a
    follower.
    """

    uris = [
        ("layered", {"uri": "layered:fast_truncate"}),
        ("table", {"uri": "table:fast_truncate"}),
    ]

    isolations = [
        ("read_committed", {"isolation": "read-committed"}),
        ("read_uncommitted", {"isolation": "read-uncommitted"}),
    ]

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, uris, isolations)
    conn_config = 'disaggregated=(role="leader"),'

    ISOLATION_MSG = "/not supported in read-committed or read-uncommitted transactions/"

    def restricted_session(self):
        """Return a session whose transaction runs at the scenario isolation."""
        return self.transaction(
            session=self.session_b,
            begin_config="isolation=" + self.isolation,
            rollback=True,
        )

    def start_uncommitted_truncate(self):
        """Leave a truncate over 30-60 uncommitted on the main session."""
        self.session.begin_transaction()
        self.truncate_on(self.session, 30, 60)

    def test_overlapping_truncate_with_ingest(self):
        # A follower with stable keys 1-100 and ingest key 45.
        self.setup_leader(keys=range_inclusive(1, 100))
        self.setup_follower(keys=[45])
        self.start_uncommitted_truncate()

        with self.auto_closing_session() as self.session_b:
            with self.restricted_session():
                self.assertRaisesWithMessage(
                    WiredTigerError,
                    lambda: self.truncate_on(self.session_b, 40, 70),
                    self.ISOLATION_MSG,
                )

    def test_overlapping_truncate_no_ingest(self):
        # A follower with stable keys 1-100 and an empty ingest table.
        self.setup_leader(keys=range_inclusive(1, 100))
        self.setup_follower()
        self.start_uncommitted_truncate()

        with self.auto_closing_session() as self.session_b:
            with self.restricted_session():
                self.assertRaisesWithMessage(
                    WiredTigerError,
                    lambda: self.truncate_on(self.session_b, 40, 70),
                    self.ISOLATION_MSG,
                )

    def test_truncate_without_conflict(self):
        # A follower with stable keys 1-100 and no truncate in progress.
        self.setup_leader(keys=range_inclusive(1, 100))
        self.setup_follower()

        with self.auto_closing_session() as self.session_b:
            with self.restricted_session():
                self.assertRaisesWithMessage(
                    WiredTigerError,
                    lambda: self.truncate_on(self.session_b, 10, 20),
                    self.ISOLATION_MSG,
                )


if __name__ == "__main__":
    wttest.run()
