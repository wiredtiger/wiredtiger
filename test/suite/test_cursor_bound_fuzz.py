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

import wiredtiger, wttest, string, random
from enum import Enum
from wtscenario import make_scenarios

class bound():
    def __init__(self, key, inclusive):
        self.key = key
        self.inclusive = inclusive

    def to_string(self):
        return "Key: " + str(self.key) + " incl: " + self.inclusive_str()

    def inclusive_str(self):
        if (self.inclusive):
            return "true"
        else:
            return "false"

class bounds():
    # Initialize with junk values.
    lower = bound(-1, False)
    upper = bound(-1, False)

    def __init__(self, lower, upper):
        self.lower = lower
        self.upper = upper

    def to_string(self):
        return "Lower: " + self.lower.to_string() + ", Upper: " + self.upper.to_string()

    def in_bounds_key(self, key):
        if (key == self.lower.key):
            if (not self.lower.inclusive):
                return False
        elif (key < self.lower.key):
            return False
        if (key == self.upper.key):
            if (not self.upper.inclusive):
                return False
        elif (key > self.upper.key):
            return False
        return True




class operations(Enum):
    UPSERT = 1
    REMOVE = 2
    #TRUNCATE = 3

class key_states(Enum):
    UPSERTED = 1
    DELETED = 2
    NONE = 3

class bound_scenarios(Enum):
    NEXT = 1
    PREV = 2
    SEARCH = 3
    SEARCH_NEAR = 4

class key():
    key_state = key_states.NONE
    data = -1
    value = "none"
    prepared = False

    def __init__(self, data, value, key_state):
        self.key_state = key_state
        self.data = data
        self.value = value

    def is_deleted(self):
        return self.key_state == key_states.DELETED

    def is_out_of_bounds(self, bound_set):
        return not bound_set.in_bounds_key(self.data)

    def is_deleted_or_oob(self, bound_set):
        return self.is_deleted() or self.is_out_of_bounds(bound_set)

    def update(self, value, key_state):
        self.value = value
        self.key_state = key_state

    def to_string(self):
        return "Key: " + str(self.data) + ", value: " + str(self.value) + ", state: " + str(self.key_state) + ", prepared: " + str(self.prepared)

    def equals(self, key, value):
        if (self.key_state == key_states.UPSERTED and self.data == key and self.value == value):
            return True
        else:
            return False

# test_cursor_bound01.py
#    Basic cursor bound API validation.
class test_cursor_bound_fuzz(wttest.WiredTigerTestCase):
    file_name = 'test_fuzz.wt'

    types = [
        ('file', dict(uri='file:')),
        #('table', dict(uri='table:'))
    ]

    data_format = [
        ('row', dict(key_format='i')),
        #('column', dict(key_format='r'))
    ]
    scenarios = make_scenarios(types, data_format)

    key_range = []

    def dump_key_range(self):
        for i in range(0, self.key_count):
            self.pr(self.key_range[i].to_string())

    def generate_value(self):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(self.value_size))

    def apply_update(self, cursor, key_id):
        value = self.generate_value()
        cursor[key_id] = value
        self.key_range[key_id].update(value, key_states.UPSERTED)
        self.pr("Updating key: " + str(key_id))

    def apply_remove(self, cursor, key_id):
        if (self.key_range[key_id].is_deleted()):
            return
        self.pr("Removing key: " + str(key_id))
        cursor.set_key(key_id)
        self.assertEqual(cursor.remove(), 0)
        self.key_range[key_id].update(None, key_states.DELETED)

    def apply_ops(self, cursor):
        op_count = self.key_count
        for i in range(0, op_count):
            op = random.choice(list(operations))
            if (op is operations.UPSERT):
                self.apply_update(cursor, i)
            elif (op is operations.REMOVE):
                self.apply_remove(cursor, i)
            else:
                raise Exception("Unhandled operation generated")

    def run_next_prev(self, bound_set, next, cursor):
        # This array gives us confidence that we have validated the full key range.
        checked_keys = []
        if (next):
            self.pr("Running scenario: NEXT")
            key_range_it = -1
            while (cursor.next() != wiredtiger.WT_NOTFOUND):
                current_key = cursor.get_key()
                current_value = cursor.get_value()
                self.pr("Cursor next walked to key: " + str(current_key) + " value: " + current_value)
                self.assertTrue(bound_set.in_bounds_key(current_key))
                self.assertTrue(self.key_range[current_key].equals(current_key, current_value))
                checked_keys.append(current_key)
                if (current_key != key_range_it + 1):
                    # Check that the key range between key_range_it and current_key isn't visible
                    for i in range(key_range_it + 1, current_key):
                        checked_keys.append(i)
                        self.pr("Determining key state of " + self.key_range[i].to_string())
                        self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
                key_range_it = current_key
            # If key_range_it is < key_count then the rest of the range was deleted
            # Remember to increment it by one to get it to the first not in bounds key.
            self.pr("key_range_it = " + str(key_range_it))
            key_range_it = key_range_it + 1
            for i in range(key_range_it, self.key_count):
                checked_keys.append(i)
                self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
        else:
            self.pr("Running scenario: PREV")
            key_range_it = self.key_count
            while (cursor.prev() != wiredtiger.WT_NOTFOUND):
                current_key = cursor.get_key()
                current_value = cursor.get_value()
                self.pr("Cursor prev walked to key: " + str(current_key) + " value: " + current_value)
                self.assertTrue(bound_set.in_bounds_key(current_key))
                self.assertTrue(self.key_range[current_key].equals(current_key, current_value))
                checked_keys.append(current_key)
                if (current_key != key_range_it - 1):
                    # Check that the key range between key_range_it and current_key isn't visible
                    for i in range(current_key + 1, key_range_it):
                        checked_keys.append(i)
                        self.pr("Determining key state of " + self.key_range[i].to_string())
                        self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
                key_range_it = current_key
            # If key_range_it is > key_count then the rest of the range was deleted
            self.pr("key_range_it = " + str(key_range_it))
            for i in range(0, key_range_it):
                checked_keys.append(i)
                self.pr("Determining key state of " + self.key_range[i].to_string())
                self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
        self.assertTrue(len(checked_keys) == self.key_count)

    def run_search(self, bound_set, cursor):
        # Choose a random key and preform a search
        rand_key = random.randrange(self.key_count)
        self.pr(str(rand_key))
        cursor.set_key(rand_key)
        ret = cursor.search()
        if (ret == wiredtiger.WT_PREPARE_CONFLICT):
            pass
        elif (ret == wiredtiger.WT_NOTFOUND):
            self.assertTrue(self.key_range[rand_key].is_deleted())
        elif (ret == 0):
            # Assert that the key exists, and is within the range.
            self.assertTrue(self.key_range[rand_key].equals(cursor.get_key(), cursor.get_value()))
            self.assertTrue(bound_set.in_bounds_key(cursor.get_key()))
        else:
            raise Exception('Unhandled error returned by search')

    def check_all_within_bounds_not_visible(self, bound_set):
        for i in range(bound_set.lower.key, bound_set.upper.key + 1):
            if (i == bound_set.lower.key and not bound_set.lower.inclusive):
                continue
            if (i == bound_set.upper.key and not bound_set.upper.inclusive):
                continue
            self.pr(self.key_range[i].to_string())
            if (not self.key_range[i].is_deleted()):
                return False
        return True

    def run_search_near(self, bound_set, cursor):
        # Choose a random key and perform a search near.
        self.session.breakpoint()
        search_key = random.randrange(self.key_count)
        cursor.set_key(search_key)
        self.pr(str(search_key))
        self.pr(bound_set.to_string())
        ret = cursor.search_near()
        if (ret == wiredtiger.WT_NOTFOUND):
            # Nothing visible within the bound range.
            # Validate.
            self.assertTrue(self.check_all_within_bounds_not_visible(bound_set))
        elif (ret == wiredtiger.WT_PREPARE_CONFLICT):
            # We ran into a prepare conflict, validate.
            pass
        else:
            key_found = cursor.get_key()
            current_key = key_found
            # Assert the value we found matches.
            # Equals also validates that the key is visible.
            self.assertTrue(self.key_range[current_key].equals(current_key, cursor.get_value()))
            if (bound_set.in_bounds_key(search_key)):
                # We returned a key within the range, validate that key is the one that should've been returned.
                if (key_found == search_key):
                    # We've already deteremined the key matches. We can return.
                    pass
                if (key_found > search_key):
                    # Walk left and validate that all isn't visible to the search key.
                    while (current_key != search_key):
                        current_key = current_key - 1
                        self.assertTrue(self.key_range[current_key].is_deleted())
                if (key_found < search_key):
                    # Walk right and validate that all isn't visible to the search key.
                    while (current_key != search_key):
                        current_key = current_key + 1
                        self.assertTrue(self.key_range[current_key].is_deleted())
            else:
                # We searched for a value outside our range, we should return whichever value
                # is closest within the range.
                if (search_key <= bound_set.lower.key):
                    # We searched to the left of our bounds. In the equals case the lower bound
                    # must not be inclusive.
                    # Validate that the we returned the nearest value to the lower bound.
                    if (bound_set.lower.inclusive):
                        self.assertTrue(key_found >= bound_set.lower.key)
                        current_key = bound_set.lower.key
                    else:
                        self.assertTrue(key_found > bound_set.lower.key)
                        current_key = bound_set.lower.key + 1
                    while (current_key != key_found):
                        self.assertTrue(self.key_range[current_key].is_deleted())
                        current_key = current_key + 1
                elif (search_key >= bound_set.upper.key):
                    # We searched to the right of our bounds. In the equals case the upper bound
                    # must not be inclusive.
                    # Validate that the we returned the nearest value to the upper bound.
                    if (bound_set.upper.inclusive):
                        self.assertTrue(key_found <= bound_set.upper.key)
                        current_key = bound_set.upper.key
                    else:
                        self.assertTrue(key_found < bound_set.upper.key)
                        current_key = bound_set.upper.key - 1
                    while (current_key != key_found):
                        self.assertTrue(self.key_range[current_key].is_deleted())
                        current_key = current_key - 1
                else:
                    raise Exception('Illegal state found in search_near')

    def run_bound_scenarios(self, bound_set, cursor):
        scenario = bound_scenarios.NEXT
        #scenario = random.choice(list(bound_scenarios))
        if (scenario is bound_scenarios.NEXT):
            self.run_next_prev(bound_set, True, cursor)
        elif (scenario is bound_scenarios.PREV):
            self.run_next_prev(bound_set, False, cursor)
        elif (scenario is bound_scenarios.SEARCH):
            self.run_search(bound_set, cursor)
        elif (scenario is bound_scenarios.SEARCH_NEAR):
            self.run_search_near(bound_set, cursor)
        else:
            raise Exception('Unhandled bound scenario chosen')

    def apply_bounds(self, cursor):
        cursor.reset()
        lower = bound(random.randrange(0, self.key_count), bool(random.getrandbits(1)))
        upper = bound(random.randrange(lower.key, self.key_count), bool(random.getrandbits(1)))
        # Prevent
        if (lower.key == upper.key):
            lower.inclusive = upper.inclusive = True
        bound_set = bounds(lower, upper)
        cursor.set_key(lower.key)
        cursor.bound("bound=lower,inclusive=" + lower.inclusive_str())
        cursor.set_key(upper.key)
        cursor.bound("bound=upper,inclusive=" + upper.inclusive_str())
        return bound_set

    def test_bound_fuzz(self):
        uri = self.uri + self.file_name
        create_params = 'value_format=S,key_format=i'

        #setup the random seed
        random.seed(10)
        self.session.create(uri, create_params)
        cursor = self.session.open_cursor(uri)

        for i in range(0, self.key_count):
            key_value = self.generate_value()
            self.key_range.append(key(i, key_value, key_states.UPSERTED))
            cursor[i] = key_value

        self.session.checkpoint()
        #self.dump_key_range()
        self.session.breakpoint()

        # Begin main loop
        for  i in range(0, self.iteration_count):
            self.pr("Iteration: " + str(i))
            bound_set = self.apply_bounds(cursor)
            #self.pr(bound_set.to_string())
            self.run_bound_scenarios(bound_set, cursor)

            # Before we apply our new operations clear the bounds.
            cursor.reset()
            self.apply_ops(cursor)
            self.session.checkpoint()
            #self.dump_key_range()

    iteration_count = 20
    value_size = 10
    key_count = 100


if __name__ == '__main__':
    wttest.run()
