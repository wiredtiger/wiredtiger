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

import wiredtiger, wttest, sys
from wtdataset import SimpleDataSet, ComplexDataSet, simple_key, simple_value
from wtscenario import make_scenarios

# test_cursor_random.py
#    Cursor next_random operations
class test_cursor_random(wttest.WiredTigerTestCase):

    # Create a large data set by inserting in-order keys. Flush content (close connection)
    # and check what samples we get.
    def test_cursor_random_inorder_checkpoint(self):

        print("Random samples in-order insert with a restart")
        nitems = 2000000
        nsamples = 100
        uri = "table:rand_cursor_test"
        ds = SimpleDataSet(self, uri, nitems)
        ds.populate()

        # Force the content out to disk
        self.reopen_conn()

        cursor = self.session.open_cursor(uri, None,
            'next_random=true,next_random_sample_size=' + str(nsamples))
        chosen_keys=[]
        for x in range(nsamples):
            self.assertEqual(cursor.next(), 0)
            chosen_keys.append(cursor.get_key())

        chosen_keys.sort()

        gaps = []
        for i in range(nsamples - 1):
            gaps.append(int(chosen_keys[i + 1]) - int(chosen_keys[i]))

        gaps.sort()
        print ("Random sample size gaps, nitems/samples = " + str(nitems / nsamples))
        print(gaps)
        cursor.close()

        cursor = self.session.open_cursor(uri, None,
            'next_random=true')
        chosen_keys=[]
        for x in range(nsamples):
            self.assertEqual(cursor.next(), 0)
            chosen_keys.append(cursor.get_key())

        chosen_keys.sort()

        gaps = []
        for i in range(nsamples - 1):
            gaps.append(int(chosen_keys[i + 1]) - int(chosen_keys[i]))

        gaps.sort()
        print ("Random no sample size gaps")
        print(gaps)

    def test_cursor_random_inorder_inmem(self):

        print("Random samples in-order insert without a restart")
        nitems = 2000000
        nsamples = 100
        uri = "table:rand_cursor_test"
        ds = SimpleDataSet(self, uri, nitems)
        ds.populate()

        cursor = self.session.open_cursor(uri, None,
            'next_random=true,next_random_sample_size=' + str(nsamples))
        chosen_keys=[]
        for x in range(nsamples):
            self.assertEqual(cursor.next(), 0)
            chosen_keys.append(cursor.get_key())

        chosen_keys.sort()

        gaps = []
        for i in range(nsamples - 1):
            gaps.append(int(chosen_keys[i + 1]) - int(chosen_keys[i]))

        gaps.sort()
        print ("Random sample size gaps, nitems/samples = " + str(nitems / nsamples))
        print(gaps)
        cursor.close()

        cursor = self.session.open_cursor(uri, None,
            'next_random=true')
        chosen_keys=[]
        for x in range(nsamples):
            self.assertEqual(cursor.next(), 0)
            chosen_keys.append(cursor.get_key())

        chosen_keys.sort()

        gaps = []
        for i in range(nsamples - 1):
            gaps.append(int(chosen_keys[i + 1]) - int(chosen_keys[i]))

        gaps.sort()
        print ("Random no sample size gaps")
        print(gaps)
