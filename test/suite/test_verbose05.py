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
#

from test_verbose01 import test_verbose_base
from wtscenario import make_scenarios
import wttest
from helper import WiredTigerCursor
import re
import math

# test_verbose05.py
# Verify checkpoint progress verbose logging emits intermediate progress messages for
# short checkpoints when page-write backoff thresholds are crossed.
@wttest.skip_for_hook("disagg", "Checkpoint progress output is different under disagg")
@wttest.skip_for_hook("tiered", "Checkpoint progress output is different under tiered storage")
class test_verbose05(test_verbose_base):

    uri = 'table:test_verbose05'
    create_config = 'key_format=S,value_format=S,allocation_size=4KB,leaf_page_max=4KB,memory_page_max=4KB'
    conn_config = 'statistics=(all),verbose=[checkpoint_progress:0]'

    size_scenarios = [
        ('small_db', dict(initial_rows=100)),
        ('large_db', dict(initial_rows=200000)),
    ]
    scenarios = make_scenarios(size_scenarios)

    def populate(self, session, row_count, seed):
        with WiredTigerCursor(session, self.uri) as cursor:
            for key in range(row_count):
                # Use long string to increase the pages
                cursor[str(key)] = seed*4000


    def test_checkpoint_progress_log_count(self):
        session = self.session
        session.create(self.uri, self.create_config)
        self.populate(session, self.initial_rows, 'x')
        session.checkpoint()
        output = self.readStdout(self.initial_rows * 100)
        progress_pattern = re.compile(
            r'WT_VERB_CHECKPOINT_PROGRESS.*Checkpoint has been running for \d+ seconds ')
        log_count = len(progress_pattern.findall(output))
        upper_limit = 100 * math.log(self.initial_rows, 10)
        self.assertLess(log_count, upper_limit, "Too many progress logs emitted: {}".format(log_count))
        self.assertGreater(log_count, min(100, self.initial_rows/100) - 1, "No progress logs emitted")
        self.close_conn()
        # The shutdown checkpoint still generate logs which may lead to test failure.
        self.cleanStdout()
