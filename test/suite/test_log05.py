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

import os, re, struct
import wttest
from helper import WiredTigerCursor

# This test simulates log file corruption by manually setting
# a log records length field to UINT32_MAX. The expected behavior is that
# the record-length validation fails, salvage is invoked, and recovery
# completes successfully. By repeating this recovery cycle, the test also
# verifies that log files do not accumulate across recovery cycles, which
# would grow the log directory without bound.
class test_log05(wttest.WiredTigerTestCase):

    uri = 'table:log05'

    conn_config = 'log=(enabled=true)'

    WT_TURTLE_FILE_NAME = "WiredTiger.turtle"
    WT_LOG_FILE = "WiredTigerLog.0000000%03d"
    WT_LOG_RE = re.compile(r'^WiredTigerLog\.\d{10}$')

    # Recovery cycles to run. A log file leaked on every failed recovery, the
    # failure this test guards against, leaves one extra file per cycle, so
    # this must be comfortably larger than max_retained_log_growth.
    test_round = 20

    # Log files that may legitimately remain on disk while checkpoints and
    # asynchronous log removal catch up. This does not grow with the number of
    # recovery cycles.
    max_retained_log_growth = 5

    def log_file_count(self):
        # Count every active log file, including the current one, so the check
        # does not depend on the file number reaching a particular value.
        # Pre-allocated and temporary log files use different prefixes.
        return sum(1 for f in os.listdir('.') if self.WT_LOG_RE.match(f))

    def extract_key_from_turtle(self, line, key):
        m = re.search(key + r'=\(\s*(\d+)\s*,\s*(\d+)\s*\)', line)
        if not m:
            return None
        file_num, offset = map(int, m.groups())
        return (file_num, offset)

    def inject_fault_to_log(self, id):
        # Read checkpoint_lsn from turtle file.
        # The checkpoint_lsn points to the byte offset (in its log file)
        # of the checkpoint log record structure.
        with open(self.WT_TURTLE_FILE_NAME, 'r') as f:
            lines = f.read().splitlines()
            self.turtle_file = lines
        for line in lines:
            value = self.extract_key_from_turtle(line, 'checkpoint_lsn')
            if value is not None:
                break
        self.assertTrue(value is not None, "Checkpoint lsn is missing in turtle file")
        _, offset = value
        # The first four bytes of each log record are parsed as a 32-bit unsigned length.
        # To simulate corruption, overwrite these bytes to UINT32_MAX.
        with open(self.WT_LOG_FILE % id, 'r+b') as f:
            f.seek(offset)
            f.write(struct.pack('<I', 0xFFFFFFFF))

    def test_duplicate_logs(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.session.begin_transaction()
        with WiredTigerCursor(self.session, self.uri) as c:
            for i in range(1000):
                c[f'key_{i}'] = f'val_{i}'
        self.session.commit_transaction()

        initial_logs = self.log_file_count()

        for i in range(self.test_round):
            self.close_conn()
            self.inject_fault_to_log(i+1)
            with self.expectedStdoutPattern("corrupted record length oversize at position"):
                self.open_conn()

        logs_count = self.log_file_count()

        # A log file leaked on every failed recovery leaves one extra file per
        # cycle. A bounded number of files may legitimately remain while
        # checkpoints and asynchronous log removal catch up; that number does
        # not grow with the cycles, so compare against the margin rather than
        # asserting an exact count.
        self.assertLessEqual(logs_count, initial_logs + self.max_retained_log_growth,
            f"Log files accumulated across recovery cycles: {logs_count} after "
            f"{self.test_round} rounds, started with {initial_logs}")
