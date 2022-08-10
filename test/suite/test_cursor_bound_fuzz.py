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

import wiredtiger, wttest, string, random, time
from wtbound import *
from enum import Enum
from wtscenario import make_scenarios

class operations(Enum):
    UPSERT = 1
    REMOVE = 2
    TRUNCATE = 3

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
    timestamp = 0

    def __init__(self, data, value, key_state, timestamp):
        self.key_state = key_state
        self.data = data
        self.value = value
        self.timestamp = timestamp

    def is_prepared(self):
        return self.prepared

    def is_deleted(self):
        return self.key_state == key_states.DELETED

    def is_out_of_bounds(self, bound_set):
        return not bound_set.in_bounds_key(self.data)

    def is_deleted_or_oob(self, bound_set):
        return self.is_deleted() or self.is_out_of_bounds(bound_set)

    def update(self, value, key_state, timestamp, prepare):
        self.value = value
        self.key_state = key_state
        self.timestamp = timestamp
        self.prepared = prepare

    def to_string(self):
        return "Key: " + str(self.data) + ", state: " + str(self.key_state) + ", prepared: " + str(self.prepared)

    def equals(self, key, value):
        if (self.key_state == key_states.UPSERTED and self.data == key and self.value == value):
            return True
        else:
            return False

# test_cursor_bound_fuzz.py
#    A python test fuzzer that generates a random key range and applies bounds to it, then runs
#    randomized operations and validates them for correctness.
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

    def key_range_iter(self):
        for i in range(self.min_key, self.max_key):
            yield i

    def dump_key_range(self):
        for i in self.key_range_iter():
            self.pr(self.key_range[i].to_string())

    def generate_value(self):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(self.value_size))

    def get_value(self):
        return self.value_array[random.randrange(self.value_array_size)]

    def get_random_key(self):
        return random.randrange(self.min_key, self.max_key)

    def apply_update(self, cursor, key_id, prepare):
        value = self.get_value()
        cursor[key_id] = value
        self.key_range[key_id].update(value, key_states.UPSERTED, self.current_ts, prepare)
        self.verbose(3, "Updating " + self.key_range[key_id].to_string())

    def apply_remove(self, cursor, key_id, prepare):
        if (self.key_range[key_id].is_deleted()):
            return
        cursor.set_key(key_id)
        self.assertEqual(cursor.remove(), 0)
        self.key_range[key_id].update(None, key_states.DELETED, self.current_ts, prepare)
        self.verbose(3, "Removing " + self.key_range[key_id].to_string())

    def apply_ops(self, session, cursor, prepare):
        op_count = self.key_count
        op = random.choice(list(operations))
        if (op is operations.TRUNCATE and self.applied_ops):
            cursor2 = session.open_cursor(self.uri + self.file_name)
            self.apply_truncate(session, cursor,cursor2, prepare)
        else:
            for i in self.key_range_iter():
                op = random.choice(list(operations))
                if (op is operations.TRUNCATE):
                    pass
                elif (op is operations.UPSERT):
                    self.apply_update(cursor, i, prepare)
                elif (op is operations.REMOVE):
                    self.apply_remove(cursor, i, prepare)
                else:
                    raise Exception("Unhandled operation generated")
        self.applied_ops = True

    def prepare_call(self, func):
        try:
            ret = func()
        except wiredtiger.WiredTigerError as e:
            if wiredtiger.wiredtiger_strerror(wiredtiger.WT_PREPARE_CONFLICT) in str(e):
                ret = wiredtiger.WT_PREPARE_CONFLICT
            else:
                raise e
        return ret

    def get_expected_key(self, bound_set, next):
        if next:
            if bound_set.lower.enabled:
                if bound_set.lower.inclusive:
                    return bound_set.lower.key
                return bound_set.lower.key + 1
            return self.min_key
        if bound_set.upper.enabled:
            if bound_set.upper.inclusive:
                return bound_set.upper.key
            return bound_set.upper.key - 1
        return self.max_key - 1

    def run_next_prev(self, bound_set, next, cursor):
        # This array gives us confidence that we have validated the full key range.
        checked_keys = []
        if (next):
            self.verbose(2, "Running scenario: NEXT")
            key_range_it = self.min_key - 1
            ret = self.prepare_call(lambda: cursor.next())

            while (ret != wiredtiger.WT_NOTFOUND and ret != wiredtiger.WT_PREPARE_CONFLICT):
                current_key = cursor.get_key()
                current_value = cursor.get_value()
                self.verbose(3, "Cursor next walked to key: " + str(current_key) + " value: " + current_value)
                self.assertTrue(bound_set.in_bounds_key(current_key))
                self.assertTrue(self.key_range[current_key].equals(current_key, current_value))
                checked_keys.append(current_key)
                # If the cursor has walked to a record that isn't +1 our current record then it
                # skipped something internally.
                # Check that the key range between key_range_it and current_key isn't visible
                if (current_key != key_range_it + 1):
                    for i in range(key_range_it + 1, current_key):
                        self.verbose(3, "Checking key is deleted or oob: " + str(i))
                        checked_keys.append(i)
                        self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
                key_range_it = current_key
                ret = self.prepare_call(lambda: cursor.next())
            key_range_it = key_range_it + 1
            if (ret == wiredtiger.WT_PREPARE_CONFLICT):
                check_key = key_range_it
                if (key_range_it == self.min_key):
                    check_key = self.get_expected_key(bound_set, next)
                self.verbose(2, "Checking if key is prepared: " + str(check_key) + " " + self.key_range[check_key].to_string())
                self.assertTrue(self.key_range[check_key].is_prepared())
                # We hit a prepare conflict we can't continue from this state.
                return
            # If key_range_it is < key_count then the rest of the range was deleted
            # Remember to increment it by one to get it to the first not in bounds key.
            for i in range(key_range_it, self.max_key):
                checked_keys.append(i)
                self.verbose(3, "Checking key is deleted or oob: " + str(i))
                self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
        else:
            self.verbose(2, "Running scenario: PREV")
            ret = self.prepare_call(lambda: cursor.prev())
            key_range_it = self.max_key
            while (ret != wiredtiger.WT_NOTFOUND and ret != wiredtiger.WT_PREPARE_CONFLICT):
                current_key = cursor.get_key()
                current_value = cursor.get_value()
                self.verbose(3, "Cursor prev walked to key: " + str(current_key) + " value: " + current_value)
                self.assertTrue(bound_set.in_bounds_key(current_key))
                self.assertTrue(self.key_range[current_key].equals(current_key, current_value))
                checked_keys.append(current_key)
                if (current_key != key_range_it - 1):
                    # Check that the key range between key_range_it and current_key isn't visible
                    for i in range(current_key + 1, key_range_it):
                        self.verbose(3, "Checking key is deleted or oob: " + str(i))
                        checked_keys.append(i)
                        self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
                key_range_it = current_key
                ret = self.prepare_call(lambda: cursor.prev())
            # If key_range_it is > key_count then the rest of the range was deleted
            key_range_it -= 1
            if (ret == wiredtiger.WT_PREPARE_CONFLICT):
                check_key = key_range_it
                if (key_range_it == self.max_key - 1):
                    check_key = self.get_expected_key(bound_set, next)
                self.verbose(2, "Checking if key is prepared: " + str(check_key) + " " + self.key_range[check_key].to_string())
                self.assertTrue(self.key_range[check_key].is_prepared())
                # We hit a prepare conflict we can't continue from this state.
                return
            for i in range(self.min_key, key_range_it + 1):
                checked_keys.append(i)
                self.verbose(3, "Checking key is deleted or oob: " + str(i))
                self.assertTrue(self.key_range[i].is_deleted_or_oob(bound_set))
        self.assertTrue(len(checked_keys) == self.key_count)

    def run_search(self, bound_set, cursor):
        # Choose a N random keys and perform a search on each
        for i in range(0, self.search_count):
            search_key = self.get_random_key()
            cursor.set_key(search_key)
            ret = self.prepare_call(lambda: cursor.search())
            if (ret == wiredtiger.WT_PREPARE_CONFLICT):
                self.assertTrue(self.key_range[search_key].is_prepared())
            elif (ret == wiredtiger.WT_NOTFOUND):
                self.assertTrue(self.key_range[search_key].is_deleted_or_oob(bound_set))
            elif (ret == 0):
                # Assert that the key exists, and is within the range.
                self.assertTrue(self.key_range[search_key].equals(cursor.get_key(), cursor.get_value()))
                self.assertTrue(bound_set.in_bounds_key(cursor.get_key()))
            else:
                raise Exception('Unhandled error returned by search')

    def check_all_within_bounds_not_visible(self, bound_set):
        for i in range(bound_set.start_range(self.min_key), bound_set.end_range(self.max_key)):
            self.verbose(3, "checking key: " +self.key_range[i].to_string())
            if (not self.key_range[i].is_deleted()):
                return False
        return True

    def run_search_near(self, bound_set, cursor):
        # Choose N random keys and perform a search near.
        for i in range(0, self.search_count):
            search_key = self.get_random_key()
            cursor.set_key(search_key)
            self.verbose(2, "Searching for key: " + str(search_key))
            ret = self.prepare_call(lambda: cursor.search_near())
            if (ret == wiredtiger.WT_NOTFOUND):
                self.verbose(2, "Nothing visible checking.")
                # Nothing visible within the bound range.
                # Validate.
            elif (ret == wiredtiger.WT_PREPARE_CONFLICT):
                # Due to the complexity of the search near logic we will simply check if there is
                # a prepared key within the range.
                found_prepare = False
                for i in range(bound_set.start_range(self.min_key), bound_set.end_range(self.max_key)):
                    if (self.key_range[i].is_prepared()):
                        found_prepare = True
                        break
                self.assertTrue(found_prepare)
                self.verbose(2, "Received prepare conflict in search near.")
            else:
                key_found = cursor.get_key()
                self.verbose(2, "Found a key: " + str(key_found))
                current_key = key_found
                # Assert the value we found matches.
                # Equals also validates that the key is visible.
                self.assertTrue(self.key_range[current_key].equals(current_key, cursor.get_value()))
                if (bound_set.in_bounds_key(search_key)):
                    # We returned a key within the range, validate that key is the one that
                    # should've been returned.
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
                    if (bound_set.lower.enabled and search_key <= bound_set.lower.key):
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
                    elif (bound_set.upper.enabled and search_key >= bound_set.upper.key):
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
        scenario = random.choice(list(bound_scenarios))

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
        lower = bound(self.get_random_key(), bool(random.getrandbits(1)), bool(random.getrandbits(1)))
        upper = bound(random.randrange(lower.key, self.max_key), bool(random.getrandbits(1)), bool(random.getrandbits(1)))
        # Prevent invalid bounds being generated.
        if (lower.key == upper.key and lower.enabled and upper.enabled):
            lower.inclusive = upper.inclusive = True
        bound_set = bounds(lower, upper)
        if (lower.enabled):
            cursor.set_key(lower.key)
            cursor.bound("bound=lower,inclusive=" + lower.inclusive_str())
        if (upper.enabled):
            cursor.set_key(upper.key)
            cursor.bound("bound=upper,inclusive=" + upper.inclusive_str())
        return bound_set

    def apply_truncate(self, session, cursor, cursor2, prepare):
        lower_key = self.get_random_key()

        if (lower_key + 1 < self.max_key):
            upper_key = random.randrange(lower_key + 1, self.max_key)
            cursor.set_key(lower_key)
            cursor2.set_key(upper_key)
            self.assertEqual(session.truncate(None, cursor, cursor2, None), 0)

            for key_id in range(lower_key, upper_key + 1):
                self.key_range[key_id].update(None, key_states.DELETED, self.current_ts, prepare)

            self.verbose(2, "Trucated keys between: " + str(lower_key) + "and" + str(upper_key))

    def get_read_ts(self):
        return self.current_ts - random.randrange(0, self.key_count)

    def test_bound_fuzz(self):
        uri = self.uri + self.file_name
        create_params = 'value_format=S,key_format={}'.format(self.key_format)
        # Reset the key range for every scenario.
        self.key_range = {}
        # Setup a reproducible random seed.
        # If this test fails inspect the file WT_TEST/results.txt and replace the time.time()
        # with a given seed. e.g.:
        # seed = 1660018908.2051475
        # Additionally this test is configured for verbose logging which can make debugging a bit
        # easier.
        seed = time.time()
        self.pr("Using seed: " + str(seed))
        random.seed(seed)
        self.session.create(uri, create_params)
        read_cursor = self.session.open_cursor(uri)

        write_session = self.setUpSessionOpen(self.conn)
        write_cursor = write_session.open_cursor(uri)


        # Initialize the value array.
        self.verbose(2, "Generating value array")
        for i in range(0, self.value_array_size):
            self.value_array.append(self.generate_value())

        # Initialize the key range.
        for i in self.key_range_iter():
            key_value = self.get_value()
            self.key_range[i] = key(i, key_value, key_states.UPSERTED, self.current_ts)
            self.current_ts += 1
            write_session.begin_transaction()
            write_cursor[i] = key_value
            write_session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.key_range[i].timestamp))

        #self.dump_key_range()
        self.session.checkpoint()
        # Begin main loop
        prepare = False
        for  i in range(0, self.iteration_count):
            self.verbose(2, "Iteration: " + str(i))
            bound_set = self.apply_bounds(read_cursor)
            self.verbose(2, "Generated bound set: " + bound_set.to_string())
            # Use the current timestamp so we don't need to track previous versions.
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(self.current_ts))
            self.run_bound_scenarios(bound_set, read_cursor)
            self.session.rollback_transaction()
            if (prepare):
                write_session.commit_transaction(
                    'commit_timestamp=' + self.timestamp_str(self.current_ts) +
                    ',durable_timestamp='+ self.timestamp_str(self.current_ts))
                self.current_ts += 1
                prepare = False
            rnd = random.uniform(0, 1)
            prepare = True#rnd <= self.prepare_frequency
            self.session.breakpoint()
            self.verbose(2, str(rnd))
            write_session.begin_transaction()
            self.apply_ops(write_session, write_cursor, prepare)
            self.verbose(2, "Prepare is " + str(prepare))
            if (prepare):
                write_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(self.current_ts))
            else:
                write_session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.current_ts))
            self.current_ts += 1
            if (i % 10 == 0):
                # Technically this is a write but easier to do it with this session.
                self.session.checkpoint()

        #self.dump_key_range()

    # A lot of time was spent generating values, to achieve some amount of randomness we pre
    # generate N values and keep them in memory.
    value_array = []
    iteration_count = 200 if wttest.islongtest() else 100
    value_size = 100000 if wttest.islongtest() else 100
    value_array_size = 20
    key_count = 10000 if wttest.islongtest() else 1000
    min_key = 1
    max_key = min_key + key_count
    current_ts = 1
    applied_ops = False
    # For each iteration we do search_count searches that way we test more cases without having to
    # generate as many key ranges.
    search_count = 20
    key_range = {}
    prepare_frequency = 1/10
    update_frequency = 2/10

if __name__ == '__main__':
    wttest.run()
