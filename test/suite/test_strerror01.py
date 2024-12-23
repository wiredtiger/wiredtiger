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
# [TEST_TAGS]
# session_api
# [END_TAGS]

import wttest
from suite_subprocess import suite_subprocess

# test_strerror01.py
#     Test generation of sub-level error codes when using calling strerror.
class test_strerror(wttest.WiredTigerTestCase, suite_subprocess):
    sub_errors = [
        (-32000, "WT_NONE: last API call was successful"),
        (-32001, "WT_COMPACTION_ALREADY_RUNNING: cannot reconfigure background compaction while it's already running"),
        (-32002, "WT_SESSION_MAX: out of sessions (including internal sessions)"),
        (-32003, "WT_CACHE_OVERFLOW: transaction rolled back because of cache overflow"),
        (-32004, "WT_WRITE_CONFLICT: conflict between concurrent operations"),
        (-32005, "WT_OLDEST_FOR_EVICTION: oldest pinned transaction ID rolled back for eviction"),
        (-32006, "WT_CONFLICT_BACKUP: the table is currently performing backup"),
        (-32007, "WT_CONFLICT_DHANDLE: another thread is accessing the table"),
        (-32008, "WT_CONFLICT_SCHEMA_LOCK: another thread is performing a schema operation"),
        (-32009, "WT_UNCOMMITTED_DATA: the table has uncommitted data and can not be dropped yet"),
        (-32010, "WT_DIRTY_DATA: the table has dirty data and can not be dropped yet"),
        (-32011, "WT_CONFLICT_TABLE_LOCK: another thread is currently reading or writing on the table"),
    ]

    def check_error_code(self, error, expected):
        assert self.session.strerror(error) == expected

    def test_strerror(self):
       for code, expected in self.sub_errors:
           self.check_error_code(code, expected)
