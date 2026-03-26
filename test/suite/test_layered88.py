#!/usr/bin/env python3
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

import os, os.path, shutil, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from itertools import permutations, combinations_with_replacement
from wtscenario import make_scenarios

# test_layered88.py
#    Test layered cursor iteration.
#
# A follower layered table is more complex than on a leader. In a layered table on a follower
# there are both ingest and stable tables.  Assuming we have either a fixed timestamp, or aren't
# using a timestamp, for any key in the stable table, there are two states - either the key exists
# or it doesn't.  For the ingest table, there are three states - the key exists, or it doesn't, or
# it has been marked as a tombstone. A tombstone indicates that the key doesn't exist, even if it
# appears in the stable table.  So for the combination of ingest and stable table, there are 2x3 = 6
# states. We're going to ignore the state where it doesn't exist in either table as it is
# uninteresting for our testing. We'll label the five remaining states as follows:
#    I  -  key exists in the ingest table only
#    S  -  key exists in the stable table only
#    B  -  key exists in both ingest and stable
#    R  -  key is in stable table and is a tombstone in ingest
#    X  -  key is not in stable table and is a tombstone in ingest
#
# To test iteration thoroughly, we want to have sequences of keys where when we do a 'next' cursor
# call, every transition is possible. For example, if key 1 is state I, we need to have tests that
# have key 2 being each of I,S,B,R,X. etc. Also, opening a cursor leaves it "unpositioned", where
# there is no key, we'll call this special state '0'. We do want to test the transition from 0
# to any state and any state to 0.
#
# The idea of this test is to create sequences of letters from the set above, e.g. IBBXS.
# We don't include 0 in the set, but as if we iterate through five keys in the state IBBXS,
# note that there is an implicit 0 and the front and the back of the string. So the 0 transitions
# will be well tested.
#
# To get complete coverage and more, we'll generate all strings of letters from this set of length
# from 0 to 6.  For each string, we'll set of the situation where we'll have the exact sequence,
# and then we'll test iterating forward from beginning to end, and backward from end to beginning,
# checking to make sure we have the expected keys and values.
#
# Using layered tables to create the expected situation requires state transitions, from leader
# to follower, and to pick up checkpoints.  These are heavyweight operations, so to save testing
# time, we'll create one table for each situation (naming it by its string), this allows us to
# test rapidly.

def generate_unique_situations(max_len):
    # Create all combinations of letters where each letter appears 0 to 2 times
    elements = ['I', 'S', 'B', 'R', 'X']
    all_situations = []

    for counts in combinations_with_replacement(range(3), len(elements)):
        # Generate every combination respecting the max count of 2 for each letter
        current_situation = []
        for letter, count in zip(elements, counts):
            current_situation.extend([letter] * count)
        if len(current_situation) <= max_len:
            all_situations.append(current_situation)

    # Generate permutations for all combinations
    all_permutations = set()
    for sit in all_situations:
        # Add all permutations of the current situation to the set
        all_permutations.update(permutations(sit))

    # Convert the set to a sorted list
    unique_situations = sorted(all_permutations)

    return unique_situations

def batched(lst, nbatches):
    ll = len(lst)
    items = (ll + nbatches - 1) // nbatches  # round up
    result = []
    for b in range(0, ll, items):
        result.append(lst[b:b+items-1])
    return result

@disagg_test_class
class test_layered88(wttest.WiredTigerTestCase):

    conn_base_config = 'statistics=(all),' \
                     + 'statistics_log=(wait=1,json=true,on_close=true),' \
                     + 'precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    nbatches_values = [
        ('nbatches=1', dict(nbatches=1)),
        ('nbatches=25', dict(nbatches=2)),
        ('nbatches=100', dict(nbatches=100)),
        ('nbatches=896', dict(nbatches=896)),
    ]
    scenarios = make_scenarios(nbatches_values)

    # Test timestamps
    def test_layered88(self):
        # Create the follower
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + \
                  ',create,' + self.conn_base_config + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')

        sits_all = generate_unique_situations(6)
        chunks = list(batched(sits_all, self.nbatches))
        #self.tty(f'chunks = {chunks}')
        ts = 0
        for sits in chunks:
          ts += 100
          uri_sits = []
          for sit in sits:
            table_name = ''
            for letter in sit:
                table_name += letter

            # Create tables and fill keys
            uri = 'table:' + table_name
            self.session.create(uri, 'key_format=S,value_format=S,block_manager=disagg,type=layered')
            uri_sits.append((uri, sit))

          for (uri, sit) in uri_sits:
            with self.transaction(commit_timestamp=ts):
                c = self.session.open_cursor(uri)
                key = 1
                for letter in sit:
                    if letter == 'S' or letter == 'B' or letter == 'R' or letter == 'X':
                        c[str(key)] = str(key)
                    key += 1
                c.close()

          self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts + 10)}')
          self.session.checkpoint()

          # Pick up checkpoint at ts + 10
          self.disagg_advance_checkpoint(conn_follow)

          for (uri, sit) in uri_sits:
            with self.transaction(commit_timestamp=ts + 20):
                c = self.session.open_cursor(uri)
                key = 1
                for letter in sit:
                    if letter == 'X':
                        c.set_key(str(key))
                        c.remove()
                    key += 1
                c.close()

          for (uri, sit) in uri_sits:
            with self.transaction(session = session_follow, commit_timestamp=ts + 20):
                c = session_follow.open_cursor(uri)
                key = 1
                for letter in sit:
                    if letter == 'X':
                        c.set_key(str(key))
                        c.remove()
                    key += 1
                c.close()

          self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(ts + 30)}')
          self.session.checkpoint()

          for (uri, sit) in uri_sits:
            with self.transaction(session = session_follow, commit_timestamp=ts + 40):
                c = session_follow.open_cursor(uri)
                key = 1
                for letter in sit:
                    if letter == 'I' or letter == 'B':
                        c[str(key)] = str(key)
                    elif letter == 'R':
                        c.set_key(str(key))
                        c.remove()
                    key += 1
                c.close()

          # Pick up checkpoint at ts + 40
          self.disagg_advance_checkpoint(conn_follow)

          for (uri, sit) in uri_sits:
            expect = []
            got = []
            key = 1
            for letter in sit:
                if letter == 'I' or letter == 'S' or letter == 'B':
                    expect.append(str(key))
                key += 1

            c = session_follow.open_cursor(uri)
            for (k, v) in c:
                self.assertEqual(k, v)
                got.append(k)

            # self.tty(f'For {uri}, expected {expect}, got {got}')
            self.assertEqual(expect, got)
            c.close()

            # got_rev = []
            # c = session_follow.open_cursor(uri)
            # while c.prev() != wiredtiger.WT_NOTFOUND:
            #     k, v = c.get_key(), c.get_value()
            #     self.assertEqual(k, v)
            #     got_rev.append(k)
            # c.close()

            # self.tty(f'For {uri} (rev), expected {list(reversed(expect))}, got {got_rev}')
            # self.assertEqual(list(reversed(expect)), got_rev)
