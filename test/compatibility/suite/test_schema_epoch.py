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

from contextlib import closing, contextmanager
from pathlib import Path

import compatibility_test
import wiredtiger
from compatibility_version import WTVersion


class test_schema_epoch(compatibility_test.CompatibilityTestCase):
    """Hand a disaggregated database between releases with schema epochs off and on.

    Each step is a fresh node that picks up the previous leader's checkpoint, sets its
    stable timestamp and steps up. Newer nodes also set the schema epoch to the stable
    timestamp, as MongoDB does. Older nodes never set it.

    The database starts on the older branch, because it cannot open one created on the
    newer branch.
    """

    checkpoint_metadata_path = Path("checkpoint_meta.txt")

    legacy_uri = "layered:legacy"
    upgrade_uri = "layered:upgrade"
    downgrade_uri = "layered:downgrade"
    reupgrade_uri = "layered:reupgrade"

    def setUp(self):
        if self.older_branch < WTVersion("mongodb-9.0"):
            self.skipTest(f"{self.older_branch.name} does not support disaggregated storage")
        super().setUp()

    @contextmanager
    def open_follower(self, branch, home):
        """Open a fresh follower on the shared page log and yield a session."""
        Path("shared").mkdir(exist_ok=True)
        Path(home).mkdir()
        Path(home, "kv_home").symlink_to("../shared")
        build_path = Path(self.branch_build_path(branch.name))
        palite_path = build_path / "ext/page_log/palite/libwiredtiger_palite.so"
        config = (
            f'create,extensions=["{palite_path}"],'
            "disaggregated=(page_log=palite,lose_all_my_data=true,role=follower)"
        )
        with (
            closing(wiredtiger.wiredtiger_open(home, config)) as connection,
            closing(connection.open_session()) as session,
        ):
            yield session

    def pick_up_checkpoint(self, session):
        """Pick up the checkpoint the previous leader saved."""
        checkpoint_meta = self.checkpoint_metadata_path.read_text(encoding="utf-8")
        session.connection.reconfigure(f'disaggregated=(checkpoint_meta="{checkpoint_meta}")')

    def set_stable(self, session, timestamp, schema_epoch=None):
        """Set the stable timestamp and, if given, the stable schema epoch."""
        config = f"stable_timestamp={timestamp:x}"
        if schema_epoch is not None:
            config += f",stable_disaggregated_schema_epoch={schema_epoch:x}"
        session.connection.set_timestamp(config)

    def checkpoint_and_store_metadata(self, session):
        """Checkpoint, step down and save the checkpoint metadata."""
        session.checkpoint()

        # Step down so closing the node does not checkpoint past the handoff.
        session.connection.reconfigure("disaggregated=(role=follower)")
        page_log = session.connection.get_page_log("palite")
        *_, checkpoint_meta = page_log.pl_get_complete_checkpoint(session)
        page_log.terminate(session)
        self.checkpoint_metadata_path.write_text(checkpoint_meta, encoding="utf-8")

    def expected_rows(self, uri):
        """Return the rows a table should hold, with values unique to that table."""
        return {f"key{i}": f"{uri}:value{i}" for i in range(100)}

    def create_populated_table(self, session, uri, commit_timestamp):
        """Create a table and insert its expected rows at the commit timestamp."""
        session.create(uri, "key_format=S,value_format=S")
        with closing(session.open_cursor(uri)) as cursor:
            session.begin_transaction()
            for key, value in self.expected_rows(uri).items():
                cursor[key] = value
            session.commit_transaction(f"commit_timestamp={commit_timestamp:x}")

    def verify_rows(self, session, *uris):
        """Assert that each table holds exactly its expected rows."""
        for uri in uris:
            with closing(session.open_cursor(uri)) as cursor:
                self.assertEqual(dict(cursor), self.expected_rows(uri), uri)

    def create_legacy_database(self):
        """Create the database on the older branch, with no schema epoch."""
        with self.open_follower(self.older_branch, "legacy") as session:
            session.connection.reconfigure("disaggregated=(role=leader)")
            self.create_populated_table(session, self.legacy_uri, commit_timestamp=10)
            self.set_stable(session, timestamp=10)
            self.checkpoint_and_store_metadata(session)

    def upgrade(self):
        """Take over the legacy database on the newer branch and publish a new table."""
        with self.open_follower(self.newer_branch, "upgrade") as session:
            self.pick_up_checkpoint(session)
            self.set_stable(session, timestamp=10, schema_epoch=10)
            session.connection.reconfigure("disaggregated=(role=leader)")

            self.verify_rows(session, self.legacy_uri)

            self.create_populated_table(session, self.upgrade_uri, commit_timestamp=20)
            session.publish(self.upgrade_uri, f"disaggregated=(schema_epoch={20:x})")
            self.set_stable(session, timestamp=20, schema_epoch=20)

            self.checkpoint_and_store_metadata(session)

    def downgrade(self):
        """Take over on the older branch and create a table without publishing it."""
        with self.open_follower(self.older_branch, "downgrade") as session:
            self.pick_up_checkpoint(session)
            self.set_stable(session, timestamp=20)
            session.connection.reconfigure("disaggregated=(role=leader)")

            self.verify_rows(session, self.legacy_uri, self.upgrade_uri)

            self.create_populated_table(session, self.downgrade_uri, commit_timestamp=30)
            self.set_stable(session, timestamp=30)

            self.checkpoint_and_store_metadata(session)

    def reupgrade(self):
        """Take over again on the newer branch and publish another table."""
        with self.open_follower(self.newer_branch, "reupgrade") as session:
            self.pick_up_checkpoint(session)
            self.set_stable(session, timestamp=30, schema_epoch=30)
            session.connection.reconfigure("disaggregated=(role=leader)")

            self.verify_rows(session, self.legacy_uri, self.upgrade_uri, self.downgrade_uri)

            self.create_populated_table(session, self.reupgrade_uri, commit_timestamp=40)
            session.publish(self.reupgrade_uri, f"disaggregated=(schema_epoch={40:x})")
            self.set_stable(session, timestamp=40, schema_epoch=40)

            self.checkpoint_and_store_metadata(session)

    def test_schema_epoch_compatibility(self):
        """Upgrade, downgrade and re-upgrade, checking each node reads earlier tables."""
        self.run_method_on_branch(self.older_branch, "create_legacy_database")
        self.run_method_on_branch(self.newer_branch, "upgrade")
        self.run_method_on_branch(self.older_branch, "downgrade")
        self.run_method_on_branch(self.newer_branch, "reupgrade")


if __name__ == "__main__":
    compatibility_test.run()
