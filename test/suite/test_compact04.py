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
# test_compact04.py
#   Test that compact doesn't reduce the file size when there are overflow values at the
#   end of file.
#
import time

import wttest, sys
from wiredtiger import stat
import queue, threading, wttest
from wtthread import checkpoint_thread, op_thread

# from build.lang.python.wiredtiger.swig_wiredtiger import WT_TS_TXN_TYPE_READ
from wtscenario import make_scenarios


def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class test_compact04(wttest.WiredTigerTestCase):
    uri = 'table:test_compact04'
    testcase_key_base = "key "
    testcase_value_base = "a really long string and a value "

    def get_num_key_values(self, time_stamp: int):
        cursor = self.session.open_cursor(self.uri, None)
        param = 'read_timestamp=' + self.timestamp_str(time_stamp)
        self.session.begin_transaction(param)
        num_values = 0
        cursor.reset()
        ret = cursor.next()
        while ret == 0:
            key = cursor.get_key()
            value = cursor.get_value()
            num_values += 1
            ret = cursor.next()
        self.session.rollback_transaction()
        cursor.close()
        return num_values

    def trigger_eviction(self, key_min: int, key_max: int):
        cursor = self.session.open_cursor(self.uri, None, "debug=(release_evict=true)")
        for index in range(key_min, key_max):
            key = self.testcase_key_base + str(index)
            cursor.set_key(key)
            cursor.search()
            cursor.reset()
            self.session.compact(self.uri)
        cursor.close()

    def perform_test(self):
        base_index = 10000000
        num_values_to_insert = 100000
        truncate_offset_min = 10000
        truncate_offset_max = 89999
        truncate_min = base_index + truncate_offset_min
        truncate_max = base_index + truncate_offset_max
        num_to_remove = truncate_max - truncate_min + 1  # +1 because truncate ranges will be inclusive
        remaining_after_truncate = num_values_to_insert - num_to_remove

        params = 'key_format=S,value_format=S,allocation_size=1024b,internal_page_max=1024b,leaf_page_max=1024b'
        self.session.create(self.uri, params)

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                ',stable_timestamp=' + self.timestamp_str(1))

        cursor = self.session.open_cursor(self.uri, None)
        max_inner = 1000
        max_outer = int(num_values_to_insert / max_inner)
        for outer in range(max_outer):
            self.assertEqual(self.session.begin_transaction(), 0)
            for inner in range(max_inner):
                index = base_index + (outer * max_inner) + inner
                key = self.testcase_key_base + str(index)
                value = self.testcase_value_base + str(index)
                cursor[key] = value
            self.assertEqual(self.session.commit_transaction(), 0)
        cursor.close()

        # done = threading.Event()
        # ckpt = checkpoint_thread(self.conn, done)
        # work_queue = queue.Queue()
        # op_threads = []

        # try:
        #     ckpt.start()
        # Truncate, with timestamp = 0x30
        # Need to trigger fast truncate, which will truncate whole pages at one.
        # Need to fast truncate an internal page as well for this test.
        self.assertEqual(self.session.begin_transaction(), 0)
        key_start = self.testcase_key_base + str(truncate_min)
        key_end = self.testcase_key_base + str(truncate_max)
        cursor_start = self.session.open_cursor(self.uri, None)
        cursor_end = self.session.open_cursor(self.uri, None)
        cursor_start.set_key(key_start)
        cursor_end.set_key(key_end)
        self.assertEqual(self.session.truncate(None, cursor_start, cursor_end, None), 0)
        self.assertEqual(self.session.commit_transaction('commit_timestamp=30'), 0)
        cursor_start.close()
        cursor_end.close()

        self.trigger_eviction(truncate_min, truncate_min + 1)

        # except Exception:
        #     # Deplete the work queue if there's an error.
        #     while not work_queue.empty():
        #         work_queue.get()
        #         work_queue.task_done()
        #     raise
        # finally:
        #     work_queue.join()
        #     done.set()
        #     for t in op_threads:
        #         t.join()
        #     ckpt.join()

        self.session.checkpoint()

        current_num_keys = self.get_num_key_values(time_stamp=0x40)
        self.assertEqual(current_num_keys, remaining_after_truncate)

        current_num_keys = self.get_num_key_values(time_stamp=0x20)
        self.assertEqual(current_num_keys, num_values_to_insert)

        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(35) +
                                ',stable_timestamp=' + self.timestamp_str(35))

        self.session.compact(self.uri)

    def test_compact04(self):
        for i in range(1):
            self.perform_test()


if __name__ == '__main__':
    wttest.run()
