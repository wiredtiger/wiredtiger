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

# For now, this is just making sure the flags are set without errors
class test_cache_evict_config01(wttest.WiredTigerTestCase):
    conn_config_common = 'cache_size=50MB,statistics=(all)'

    conn_config_values = [
        ('default_enabled', dict(
            enabled=True,
            inc_eviction=True,
            scrub_under_limit=True,
            conn_config=f'{conn_config_common},eviction_app_threads=[enabled=true,incremental_app_eviction=true,scrub_evict_under_target_limit=true]'
        )),
        ('disabled_all', dict(
            enabled=False,
            inc_eviction=False,
            scrub_under_limit=False,
            conn_config=f'{conn_config_common},eviction_app_threads=[enabled=false,incremental_app_eviction=false,scrub_evict_under_target_limit=false]'
        )),
        ('scrub_only', dict(
            enabled=True,
            inc_eviction=False,
            scrub_under_limit=True,
            conn_config=f'{conn_config_common},eviction_app_threads=[enabled=true,incremental_app_eviction=false,scrub_evict_under_target_limit=true]'
        )),
        ('incr_only', dict(
            enabled=True,
            inc_eviction=True,
            scrub_under_limit=False,
            conn_config=f'{conn_config_common},eviction_app_threads=[enabled=true,incremental_app_eviction=true,scrub_evict_under_target_limit=false]'
        )),
    ]

    scenarios = make_scenarios(conn_config_values)

    def test_eviction_app_threads(self):
        # Open a fresh connection with the scenario config.
        self.conn.close()
        self.conn = self.wiredtiger_open(".", self.conn_config)

        # Open statistics cursor.
        stat_cursor = self.session.open_cursor('statistics:')
        # For now, we just verify that the connection is established with
        # the given config (no parse errors, no startup failure).
        stat_cursor.close()

        # Create a table, insert some data, ensure no errors.
        uri = 'table:eviction01'
        self.session.create(uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(10):
            cursor[i] = "value" + str(i)
        cursor.close()
