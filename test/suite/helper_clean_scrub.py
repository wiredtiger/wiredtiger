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

# helper_clean_scrub.py
#       Shared setup for clean-scrub eviction tests.

from wtscenario import make_scenarios

# Two scenarios covering both ways the feature can fire:
#   - debug:      debug_mode.clean_scrub forces saves regardless of cache pressure;
#   - production: only the eviction config is set, so saves and scrubs require the same
#                 updates-pressure path real workloads hit.
# evict_walk_full is on in both scenarios so the eviction walk is deterministic in tests.
clean_scrub_scenarios = make_scenarios([
    ('debug', dict(extra_config='debug_mode=(clean_scrub=true,evict_walk_full=true)')),
    ('production', dict(extra_config='debug_mode=(evict_walk_full=true)')),
])

# Mixed in alongside wttest.WiredTigerTestCase to share conn_config, the populate helper,
# and the standard workload-size constants. Subclasses set scenarios = clean_scrub_scenarios
# and a per-file uri.
class CleanScrubBase:
    nrows = 10000
    value_size = 500

    def conn_config(self):
        return ('cache_size=50MB,statistics=(all),'
                'eviction=(clean_scrub_eviction=true),checkpoint=(wait=0),' + self.extra_config)

    def populate(self, start, end, value_char='a'):
        cursor = self.session.open_cursor(self.uri)
        for i in range(start, end):
            cursor[i] = value_char * self.value_size
        cursor.close()
