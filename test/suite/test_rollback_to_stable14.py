#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
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

from test_rollback_to_stable01 import test_rollback_to_stable_base
from wtdataset import SimpleDataSet

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable14
# Test history store operations conflicting with rollback to stable. We need to ensure that we're
# blocking eviction prior to rollback to stable.
class test_rollback_to_stable14(test_rollback_to_stable_base):
    session_config = 'isolation=snapshot'
    prepare = False

    def test_rollback_to_stable(self):
        nrows = 1000

        uri = 'table:rollback_to_stable14'
        ds = SimpleDataSet(
            self, uri, 0, key_format='i', value_format='S', config='log=(enabled=false)')
        ds.populate()

        for i in range(1, 1000):
            # Generate a value and timestamp based off the index.
            value = str(i) * 100
            ts = i * 10

            # Perform updates.
            self.large_updates(uri, value, ds, nrows, ts)

            # Every hundred updates, lets run rollback to stable. This is likely to happen during
            # a history store eviction at least once.
            if i % 100 == 0:
                # Put the timestamp backwards so we can rollback the updates we just did.
                stable_ts = (i - 1) * 10
                self.conn.set_timestamp('stable_timestamp=' + timestamp_str(stable_ts))
                with self.expectedStdoutPattern('.*'):
                    self.conn.rollback_to_stable()
