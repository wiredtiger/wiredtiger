#!/usr/bin/env python
#
# Public Domain 2014-2019 MongoDB, Inc.
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

import random, string, time
import wiredtiger, wttest
from wtscenario import make_scenarios

r = random.Random(42) # Make things repeatable

# test_calc_modify_overhead.py
#   Test the wiredtiger_calc_modify API overhead when the result fails
#   to satisfy the modify operation.
#
# Try many combinations of:
# - data size
# - data randomness ('a' * N, repeated patterns, uniform random)
# - number and type of modifications (add, replace)
# - space between the modifications
#
# Check that wiredtiger_calc_modify finds a set of modifies when the edit
# difference is larger than limits, it is okay to fail. Here we are trying
# identify how much overhead because of it when it fails. There may be limited
# scenarios where wiredtiger_calc_modify function may pass, those are just
# ignored.

class test_calc_modify_overhead(wttest.WiredTigerTestCase):
    uri = 'table:test_calc_modify_overhead'

    # operation types
    ADD = 1
    REPLACE = 2

    valuefmt = [
        ('item', dict(valuefmt='u')),
        ('string', dict(valuefmt='S')),
    ]
    scenarios = make_scenarios(valuefmt)

    def mkstring(self, size, repeat_size=1):
        choices = string.ascii_letters + string.digits
        if self.valuefmt == 'S':
            pattern = ''.join(r.choice(choices) for _ in range(repeat_size))
        else:
            pattern = b''.join(bytes([r.choice(choices.encode())]) for _ in range(repeat_size))
        return (pattern * int(((size + repeat_size - 1) // repeat_size)))[:size]

    def one_test(self, c, k, oldsz, repeatsz, nmod, maxdiff, size_test):
        oldv = self.mkstring(oldsz, repeatsz)

        func_maxdiff = maxdiff
        func_nmod = nmod

        # if the test is to verify the number of modifications reaching the maximum limit,
        # then increase the maximum modification size that is passed to the
        # wiredtiger_calc_modify_function.
        #
        # if the test is to verify the total modification size reaching the maximum limit,
        # then increase the number of modifications value that is passed to the
        # wiredtiger_calc_modify function.
        if size_test == 0:
            func_maxdiff = maxdiff + 10
        else:
            func_nmod = nmod + 10

        self.pr("func maxdiff: %d" % func_maxdiff)
        self.pr("func nmod: %d" % func_nmod)

        lengths = [maxdiff/nmod for i in range(nmod)]
        offsets = sorted(r.sample(range(oldsz), nmod))
        modsizes = [maxdiff/nmod for i in range(nmod)]
        modtypes = [r.choice((self.ADD, self.REPLACE)) for _ in range(nmod)]

        self.pr("offsets: %s" % offsets)
        self.pr("modsizes: %s" % modsizes)
        self.pr("lengths: %s" % lengths)
        self.pr("modtypes: %s" % modtypes)

        orig = oldv
        newv = '' if self.valuefmt == 'S' else b''
        for i in range(nmod):
            if i > 0:
                offset = offsets[i] - offsets[i-1]
            else:
                offset = offsets[i]

            newv += orig[:offset]
            orig = orig[offset:]
            if modtypes[i] == self.ADD:
                newv += self.mkstring(lengths[i], r.randint(1, lengths[i]))
            elif modtypes[i] == self.REPLACE:
                newv += self.mkstring(lengths[i], r.randint(1, lengths[i]))
                orig = orig[lengths[i]:]
        newv += orig

        self.pr("oldv: %s" % oldv)
        self.pr("newv: %s" % newv)

        start_time = time.time()
        try:
            mods = wiredtiger.wiredtiger_calc_modify(None, oldv, newv, func_maxdiff, func_nmod)
            self.pr("calculated mods: %s" % mods)
            self.pr("wiredtiger_calc_modify is success")
            return
        except wiredtiger.WiredTigerError:
            c[k] = oldv
            self.session.begin_transaction('isolation=snapshot')
            c.set_key(k)
            c.set_value(newv)
            c.update()
            self.session.commit_transaction()
        modify_fail_update_elapsed_time = time.time() - start_time

        start_time = time.time()

        c[k] = oldv
        self.session.begin_transaction('isolation=snapshot')
        c.set_key(k)
        c.set_value(newv)
        c.update()
        self.session.commit_transaction()

        normal_update_elapsed_time = time.time() - start_time

        self.pr("normal update elapsed time: %f seconds" % normal_update_elapsed_time)
        self.pr("modify fail update elapsed time: %f seconds" % modify_fail_update_elapsed_time)
        self.pr("overhead time: %f seconds" % (modify_fail_update_elapsed_time - normal_update_elapsed_time))

    def test_calc_modify(self):
        self.session.create(self.uri, 'key_format=i,value_format=' + self.valuefmt)
        c = self.session.open_cursor(self.uri)

        for size in range(1000, 10000):
            # The total size of modifications reach 10% of total document size, but less than total number of modifications.
            repeats = r.randint(1,size)
            nmods = 10
            maxdiff = size/10
            size_test = 1
            self.pr("size %s, repeats %s, nmods %s, maxdiff %s, size_test %s" % (size, repeats, nmods, maxdiff, size_test))
            self.one_test(c, size, size, repeats, nmods, maxdiff, size_test)

            # The number of modifications reach max number of modifications, but under maximum modification size.
            repeats = r.randint(1,size)
            nmods = 16
            maxdiff = size/20
            size_test = 0
            self.pr("size %s, repeats %s, nmods %s, maxdiff %s size_test %s" % (size, repeats, nmods, maxdiff, size_test))
            self.one_test(c, size, size, repeats, nmods, maxdiff, size_test)

if __name__ == '__main__':
    wttest.run()
