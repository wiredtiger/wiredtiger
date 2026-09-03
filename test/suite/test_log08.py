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

# test_log08.py
#   A log cursor that salvages a corrupt record at the end of the log must end
#   with WT_NOTFOUND, not an error. Recovery walks the log with this cursor at
#   open, so an error fails the whole open. Without zero_fill, preallocation on
#   macOS can leave such a record after the last real one.
class test_log08(wttest.WiredTigerTestCase):
    conn_config = 'log=(enabled,file_max=100K,zero_fill=false)'
    uri = 'table:test_log08'

    def test_log_cursor_after_salvage(self):
        # One committed transaction, flushed so the log is complete on disk.
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(1, 51):
            cursor[i] = 'value'
        self.session.commit_transaction()
        cursor.close()
        self.session.log_flush('sync=on')

        # A stray byte in the record length of the slot right after the last
        # record. Records are 128 byte aligned, so the scan reads it as a header
        # whose checksum does not match and salvages it.
        with open('WiredTigerLog.0000000001', 'r+b') as f:
            used = len(f.read().rstrip(b'\x00'))
            f.seek((used + 127) // 128 * 128)
            f.write(b'\x08')

        # Walk off the end of the log. The salvage truncates the file and the
        # cursor must then report end of log rather than fail.
        log_cursor = self.session.open_cursor('log:')
        with self.expectedStdoutPattern('record len corruption 0x8'):
            while log_cursor.next() == 0:
                pass
        log_cursor.close()
