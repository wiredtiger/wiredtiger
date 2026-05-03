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

import wttest
from wtscenario import make_scenarios

# test_cache_evict_config03.py
#
# Verify that the eviction.two_phase_eviction config option is accepted at
# connection open time and at reconfigure time, and that the connection
# remains functional after each change. The option defaults to true and
# enables two-phase eviction (reconcile under hazard pointer in phase 1,
# acquire exclusive lock only for the swap-out in phase 2).
class test_cache_evict_config03(wttest.WiredTigerTestCase):
    """Test config acceptance for eviction.two_phase_eviction."""

    # Test that the option is parsed correctly when supplied at open time.
    scenarios = make_scenarios([
        ('default',     dict(initial_cfg='')),
        ('open_true',   dict(initial_cfg=',eviction=[two_phase_eviction=true]')),
        ('open_false',  dict(initial_cfg=',eviction=[two_phase_eviction=false]')),
    ])

    uri = 'table:test_cache_evict_config03'
    nrows = 50

    def conn_config(self):
        return f'cache_size=50MB,statistics=(all){self.initial_cfg}'

    def _populate(self):
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            cursor[i] = 'value_' + str(i)
        cursor.close()

    def _verify_all(self):
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0)
            self.assertEqual(cursor.get_value(), 'value_' + str(i))
        cursor.close()

    def test_two_phase_config_accepted(self):
        """Opening a connection with two_phase_eviction=true/false leaves the
        connection fully operational for reads and writes."""
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self._populate()
        self._verify_all()

    def test_two_phase_reconfig(self):
        """eviction.two_phase_eviction can be toggled at runtime without
        disrupting ongoing reads or writes."""
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self._populate()

        # Enable two-phase eviction.
        self.conn.reconfigure('eviction=[two_phase_eviction=true]')
        self._verify_all()

        # Disable two-phase eviction (fall back to single-phase model).
        self.conn.reconfigure('eviction=[two_phase_eviction=false]')
        self._verify_all()

        # Re-enable; connection must still be healthy.
        self.conn.reconfigure('eviction=[two_phase_eviction=true]')
        self._verify_all()

    def test_two_phase_combined_with_other_eviction_flags(self):
        """two_phase_eviction can be combined with other eviction sub-options
        in a single reconfigure call."""
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self._populate()

        configs = [
            'eviction=[two_phase_eviction=true,prefer_scrub_eviction=false]',
            'eviction=[two_phase_eviction=false,prefer_scrub_eviction=true]',
            'eviction=[two_phase_eviction=true,skip_update_obsolete_check=true]',
            'eviction=[two_phase_eviction=false,skip_update_obsolete_check=false]',
            'eviction=[two_phase_eviction=true,incremental_app_eviction=true]',
            'eviction=[two_phase_eviction=false,incremental_app_eviction=false]',
        ]
        for cfg in configs:
            self.conn.reconfigure(cfg)
            self._verify_all()


if __name__ == '__main__':
    wttest.run()
