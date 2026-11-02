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
# verifies that log files are removed within a few cycles rather than
# accumulating and growing the log directory without bound.
class test_log05(wttest.WiredTigerTestCase):

    uri = 'table:log05'

    conn_config = 'log=(enabled=true)'

    WT_TURTLE_FILE_NAME = "WiredTiger.turtle"
    WT_LOG_FILE = "WiredTigerLog.0000000%03d"
    WT_LOG_RE = re.compile(r'^WiredTigerLog\.(\d{10})$')

    # Recovery cycles to run. A log file leaked on every failed recovery, the
    # failure this test guards against, would survive all of them, so this must
    # be comfortably larger than max_retention_rounds.
    test_round = 20

    # A log file may remain on disk while checkpoints and asynchronous log
    # removal catch up, but it must be removed within this many recovery
    # cycles. Normally a file is removed one cycle after it is superseded; the
    # worst lag observed under CI load is three cycles. Allow a small margin
    # while staying far below test_round, so a file that is never removed
    # still fails the test.
    max_retention_rounds = 5

    def log_file_numbers(self):
        # Return the numbers of the active log files, in order. Pre-allocated
        # and temporary log files use different prefixes.
        return sorted(int(self.WT_LOG_RE.match(f).group(1))
            for f in os.listdir('.') if self.WT_LOG_RE.match(f))

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

        for i in range(self.test_round):
            self.close_conn()
            self.inject_fault_to_log(i+1)
            with self.expectedStdoutPattern("corrupted record length oversize at position"):
                self.open_conn()

            # A log file leaked by a failed recovery is never superseded by
            # removal. Check every cycle that no file outlives the retention
            # deadline, rather than asserting a total count, so the check does
            # not depend on how many files a build retains at a given instant.
            nums = self.log_file_numbers()
            current = nums[-1]
            stale = [n for n in nums if current - n > self.max_retention_rounds]
            self.assertEqual(stale, [],
                f"Log files survived more than {self.max_retention_rounds} "
                f"recovery cycles: {stale}")
