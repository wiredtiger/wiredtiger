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
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wiredtiger import stat
from wtscenario import make_scenarios

# test_layered42.py
#    Test blocking dirty internal page eviction for disaggregated storage.
@disagg_test_class
class test_layered42(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = ',cache_size=50MB,disaggregated=(page_log=palm),disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_layered41', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config

    def test_layered42(self):
        uri = "layered:test_layered42"
        self.session.create(uri, "key_format=i,value_format=S")

        c = self.session.open_cursor(uri)
        for i in range(0, 5000000):
            self.session.begin_transaction()
            c[i] = str(i)
            self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(1)}')
        
        stat_cursor = self.session.open_cursor('statistics:')
        self.assertGreater(stat_cursor[stat.conn.cache_eviction_blocked_disagg_dirty_internal_page][2], 0)
        stat_cursor.close()
