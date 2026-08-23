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

# When step-up creates a missing stable constituent, its immutable file
# settings must match those of a stable constituent created in the leader
# role.

from helper_disagg import DisaggSchemaEpochMixin
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wttest import WiredTigerTestCase


@disagg_test_class
class test_layered_schema31(WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_config = 'disaggregated=(role="leader",lose_all_my_data=true)'

    # Create the table with a non-default value for every user-settable
    # immutable file setting, so the comparison exercises the full set of
    # fields the rebuild must preserve.
    # Encryption belongs to the same set but needs an encryptor extension, so
    # it is left out. The storage tier exercises the recursive merge of the
    # disaggregated category when the rebuild overrides the page log.
    base_config = (
        "key_format=i,value_format=S,"
        "allocation_size=512B,"
        "block_allocation=first,"
        "block_compressor=snappy,"
        "block_manager=disagg,"
        "checksum=unencrypted,"
        "dictionary=100,"
        "disaggregated=(storage_tier=cold),"
        "internal_key_truncate=false,"
        "internal_page_max=8KB,"
        "key_gap=20,"
        "leaf_key_max=256,"
        "leaf_page_max=8KB,"
        "leaf_value_max=1KB,"
        "memory_page_image_max=64KB,"
        "memory_page_max=1MB,"
        "prefix_compression=true,"
        "prefix_compression_min=8,"
        "split_deepen_min_child=100,"
        "split_deepen_per_child=50,"
        "split_pct=80"
    )

    uri_scenarios = [
        (
            "layered",
            {
                "uri": f"layered:{test_name}",
                "ref_uri": f"layered:{test_name}_ref",
                "table_config": base_config,
            },
        ),
        (
            "table",
            {
                "uri": f"table:{test_name}",
                "ref_uri": f"table:{test_name}_ref",
                "table_config": base_config + ",type=layered",
            },
        ),
    ]

    def conn_extensions(self, extlist):
        self.add_scenario_config()
        extlist.extension("compressors", "snappy")
        return self.disagg_conn_extensions(extlist)

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, uri_scenarios)

    def create_on_follower_then_step_up(self, config):
        """
        Create and publish the table in the follower role, then step up to
        rebuild the missing stable constituent. A twin table created in the
        leader role first provides the reference stable constituent row the
        rebuild must reproduce.
        """
        self.set_stable_epoch(1)
        self.session.create(self.ref_uri, config)
        self.step_down()

        # A follower-era create has no stable constituent: only the ingest
        # constituent and the layered table entry exist until the next step-up.
        self.session.create(self.uri, config)
        self.publish(self.uri, 5)
        self.assertFalse(
            self.stable_exists_locally(
                self.conn, self.layered_stable_uri(self.uri)
            )
        )

        self.step_up()

    def create_on_follower_then_step_up_legacy(self):
        """
        The same flow without schema epochs, exercising the legacy step-up
        path that reconstructs the missing stable constituent from the
        ingest constituent.
        """
        self.session.create(self.ref_uri, self.table_config)
        self.step_down()

        self.session.create(self.uri, self.table_config)
        self.assertFalse(
            self.stable_exists_locally(
                self.conn, self.layered_stable_uri(self.uri)
            )
        )

        self.step_up()

    def layered_stable_uri(self, uri):
        """Return the stable constituent URI for a layered: or table: URI."""
        return "file:" + uri.split(":", 1)[1] + ".wt_stable"

    def read_stable_config(self, conn, stable_uri):
        """Return the stable constituent's local create configuration."""
        session = conn.open_session("")
        cursor = session.open_cursor("metadata:create")
        cursor.set_key(stable_uri)
        self.assertEqual(
            cursor.search(), 0, f"no local metadata for {stable_uri}"
        )
        config = cursor.get_value()
        cursor.close()
        session.close()
        return config

    def stable_exists_locally(self, conn, stable_uri):
        """
        Return True if the stable constituent has a row in conn's local
        metadata.
        """
        session = conn.open_session("")
        cursor = session.open_cursor("metadata:")
        cursor.set_key(stable_uri)
        found = cursor.search() == 0
        cursor.close()
        session.close()
        return found

    def assert_matches_reference(self, rebuilt_config, ref_uri=None):
        """
        Assert a stable constituent's create configuration matches the
        leader-created reference.
        """
        reference_config = self.read_stable_config(
            self.conn, self.layered_stable_uri(ref_uri or self.ref_uri)
        )
        self.assertEqual(rebuilt_config, reference_config)

    def check_rebuild_keeps_file_settings(self):
        """
        The rebuilt stable constituent keeps the create-time file settings.
        """
        self.assert_matches_reference(
            self.read_stable_config(
                self.conn, self.layered_stable_uri(self.uri)
            )
        )

    def test_stepup_rebuild_keeps_file_settings(self):
        self.create_on_follower_then_step_up(self.table_config)
        self.set_stable_epoch(10)
        self.check_rebuild_keeps_file_settings()

    def test_stepup_rebuild_keeps_file_settings_legacy(self):
        self.create_on_follower_then_step_up_legacy()
        self.check_rebuild_keeps_file_settings()

    def test_stepup_rebuild_keeps_default_config(self):
        """
        Verify step-up uses the saved configuration rather than the
        ingest-derived fallback.
        """
        config = self.table_config.replace("block_manager=disagg,", "")
        self.create_on_follower_then_step_up(config)
        self.check_rebuild_keeps_file_settings()
