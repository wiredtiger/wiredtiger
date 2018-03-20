#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
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

import fnmatch, os, time
import wiredtiger, wttest
from wtdataset import SimpleDataSet

# test_bug019.py
#    Test that pre-allocating log files only pre-allocates a small number.
class test_bug019(wttest.WiredTigerTestCase):
    conn_config = 'log=(enabled,file_max=100K)'
    uri = "table:bug019"
    entries = 100000

    # There was a bug where pre-allocated log files accumulated on
    # Windows systems due to an issue with the directory list code.
    # Make sure the number of pre-allocated log files remains constant.
    def populate(self, start, nentries):
        c = self.session.open_cursor(self.uri, None, None)
        end = start + nentries
        for i in range(start, end):
            c[i] = i
        c.close()

    def test_bug019(self):
        # Create a table just to write something into the log.  Sleep
        # to give the worker thread a chance to run.
        self.session.create(self.uri, 'key_format=i,value_format=i')
        self.populate(0, self.entries)
        self.session.checkpoint()
        # After populating, sleep to allow pre-allocation to respond.
        # Then loop a few times making sure pre-allocation is keeping
        # up but not continually adding more files.
        time.sleep(1)
        prep_logs = len(fnmatch.filter(os.listdir('.'), "*Prep*"))

        for i in range(1,5):
            self.populate(self.entries * i, self.entries)
            new_prep = len(fnmatch.filter(os.listdir('.'), "*Prep*"))
            self.assertTrue(new_prep <= prep_logs)
            self.session.checkpoint()

if __name__ == '__main__':
    wttest.run()
