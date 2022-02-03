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
# test_timestamp26.py
#   Timestamps: assert commit settings
#

import wiredtiger, wttest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# Test assert/verbose always/never settings when associated with write_timestamp_usage.
class test_timestamp26_always_never(wttest.WiredTigerTestCase):
    results = [
        ('always-assert', dict(result='always-assert',
            config='write_timestamp_usage=always,assert=(write_timestamp=on)')),
        ('always-verbose', dict(result='always-verbose',
            config='write_timestamp_usage=always,verbose=(write_timestamp=on)')),
        ('default-usage-off', dict(result='none', config='assert=(write_timestamp=off)')),
        ('default-usage-on', dict(result='none', config='assert=(write_timestamp=on)')),
        ('never-assert', dict(result='never-assert',
            config='write_timestamp_usage=never,assert=(write_timestamp=on)')),
        ('never-verbose', dict(result='never-verbose',
            config='write_timestamp_usage=never,verbose=(write_timestamp=on)')),
    ]
    commit_ts = [
        ('yes', dict(commit_ts=True)),
        ('no', dict(commit_ts=False)),
    ]
    with_ts = [
        ('yes', dict(with_ts=True)),
        ('no', dict(with_ts=False)),
    ]
    types = [
        ('fix', dict(key_format='r', value_format='8t')),
        ('row', dict(key_format='S', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ]
    scenarios = make_scenarios(types, commit_ts, with_ts, results)

    def test_always_never(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        # Open the object, configuring write_timestamp usage.
        uri = 'table:ts'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format) +
            ',' + self.config)

        c = self.session.open_cursor(uri)
        self.session.begin_transaction()
        c[ds.key(7)] = ds.value(8)

        # Commit with a timestamp.
        if self.with_ts:
            # Check both an explicit timestamp set and a set at commit.
            commit_ts = 'commit_timestamp=' + self.timestamp_str(10)
            if not self.commit_ts:
                self.session.timestamp_transaction(commit_ts)
                commit_ts = ''

            msg = 'set when disallowed'
            if not self.result.startswith('never'):
                self.session.commit_transaction(commit_ts)
            elif self.result == 'never-verbose':
                with self.expectedStderrPattern(msg):
                    self.session.commit_transaction(commit_ts)
            else:
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.commit_transaction(commit_ts), '/' + msg + '/')

        # Commit without a timestamp.
        else:
            msg = 'timestamp required by table'
            if not self.result.startswith('always'):
                self.session.commit_transaction()
            elif self.result == 'always-verbose':
                with self.expectedStderrPattern(msg):
                    self.session.commit_transaction()
            else:
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.commit_transaction(), '/' + msg + '/')

# Test assert/verbose read timestamp settings.
class test_timestamp26_read_timestamp(wttest.WiredTigerTestCase):
    results = [
        ('always-assert', dict(result='always-assert', config='assert=(read_timestamp=always)')),
        ('always-verbose', dict(result='always-verbose', config='verbose=(read_timestamp=always)')),
        ('default', dict(result='default', config='')),
        ('never-assert', dict(result='never-assert', config='assert=(read_timestamp=never)')),
        ('never-verbose', dict(result='never-verbose', config='verbose=(read_timestamp=never)')),
        ('none', dict(result='none', config='assert=(read_timestamp=none)')),
    ]
    types = [
        ('fix', dict(key_format='r', value_format='8t')),
        ('row', dict(key_format='S', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ]
    scenarios = make_scenarios(types, results)

    def test_read_timestamp(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        # Open the object, configuring timestamp usage.
        uri = 'table:ts'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format) +
            ',' + self.config)

        c = self.session.open_cursor(uri)
        key = ds.key(10)
        value = ds.value(10)

        # Insert a data item at a timestamp (although it doesn't really matter).
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(10))
        c[key] = value
        self.session.timestamp_transaction()
        self.session.commit_transaction()

        # Try reading without a timestamp.
        self.session.begin_transaction()
        c.set_key(key)
        msg = 'read timestamps required and none set'
        if not self.result.startswith('always'):
            self.assertEquals(c.search(), 0)
            self.assertEqual(c.get_value(), value)
        elif self.result == 'always-verbose':
            with self.expectedStderrPattern(msg):
                self.assertEquals(c.search(), 0)
        else:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda:self.assertEquals(c.search(), EINVAL), '/' + msg + '/')
        self.session.rollback_transaction()

        # Try reading with a timestamp.
        self.session.begin_transaction()
        self.session.timestamp_transaction('read_timestamp=20')
        c.set_key(key)
        msg = 'read timestamps disallowed'
        if self.result.startswith('always'):
            self.assertEquals(c.search(), 0)
            self.assertEqual(c.get_value(), value)
        elif self.result == 'never-verbose':
            with self.expectedStderrPattern(msg):
                self.assertEquals(c.search(), 0)
        elif self.result == 'never-assert':
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda:self.assertEquals(c.search(), EINVAL), '/' + msg + '/')
        else:
            self.assertEquals(c.search(), 0)
        self.session.rollback_transaction()

# Test alter of timestamp settings.
class test_timestamp26_alter(wttest.WiredTigerTestCase):
    start = [
        ('always', dict(init_always=True)),
        ('never', dict(init_always=False)),
    ]
    types = [
        ('fix', dict(key_format='r', value_format='8t')),
        ('row', dict(key_format='S', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ]
    scenarios = make_scenarios(types, start)

    # Perform and operation and check the result for failure.
    def check(self, ds, uri, willfail):
        c = self.session.open_cursor(uri)
        self.session.begin_transaction()
        c[ds.key(10)] = ds.value(10)
        if willfail:
            msg = '/timestamp required by table configuration/'
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda:self.assertEquals(self.session.commit_transaction(), 0), msg)
        else:
            self.session.commit_transaction()
        c.close()

    def test_alter(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        if self.init_always:
            start = 'always'
            switch = 'never'
        else:
            start = 'never'
            switch = 'always'

        # Open the object, configuring the initial timestamp usage.
        # Check it.
        # Switch the object to the opposite usage.
        # Check it.
        uri = 'table:ts'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format) +
            ',' + 'write_timestamp_usage={}'.format(start) + ',assert=(write_timestamp=on)')
        self.check(ds, uri, self.init_always)
        self.session.alter(uri, 'write_timestamp_usage={}'.format(switch))
        self.check(ds, uri, not self.init_always)

# Test timestamp settings with inconsistent updates.
class test_timestamp26_inconsistent(wttest.WiredTigerTestCase):
    types = [
        ('fix', dict(key_format='r', value_format='8t')),
        ('row', dict(key_format='S', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ]
    assert_or_verbose = [
        ('assert', dict(set_assert=True)),
        ('verbose', dict(set_assert=False)),
    ]
    scenarios = make_scenarios(types, assert_or_verbose)

    def test_ordered(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        # Create the table without the key consistency checking turned on.
        # Create a few items breaking the rules. Then alter the setting and
        # verify the inconsistent usage is detected.
        uri = 'table:ts'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format))

        c = self.session.open_cursor(uri)
        key = ds.key(10)

        # Insert a data item at timestamp 2.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(2))
        c[key] = ds.value(10)
        self.session.commit_transaction()

        # Update the data item at timestamp 1.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(1))
        c[key] = ds.value(11)
        self.session.commit_transaction()

        key = ds.key(12)

        # Insert a non-timestamped item, then update with a timestamp and then without a timestamp.
        self.session.begin_transaction()
        c[key] = ds.value(12)
        self.session.commit_transaction()

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(2))
        c[key] = ds.value(13)
        self.session.commit_transaction()

        self.session.begin_transaction()
        c[key] = ds.value(14)
        self.session.commit_transaction()

        # Now alter the setting and make sure we detect incorrect usage. We must move the oldest
        # timestamp forward in order to alter, otherwise alter will fail with EBUSY.
        c.close()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        config = 'assert=(write_timestamp=on)' if self.set_assert \
            else 'verbose=(write_timestamp=on)'
        self.session.alter(uri, 'write_timestamp_usage=ordered,' + config)

        c = self.session.open_cursor(uri)
        key = ds.key(15)

        # Detect decreasing timestamp.
        self.session.begin_transaction()
        c[key] = ds.value(15)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(15))

        msg = 'with an older timestamp'
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(14))
        c[key] = ds.value(16)
        if self.set_assert:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(), '/' + msg + '/')
        else:
            with self.expectedStderrPattern(msg):
                self.session.commit_transaction()

        # Detect not using a timestamp.
        msg = 'use timestamps once they are first used'
        self.session.begin_transaction()
        c[key] = ds.value(17)
        if self.set_assert:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(), '/' + msg + '/')
        else:
            with self.expectedStderrPattern(msg):
                self.session.commit_transaction()

        # Now alter the setting again and detection is off.
        c.close()
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(20))
        self.session.alter(uri, 'assert=(write_timestamp=off),verbose=(write_timestamp=off)')

        c = self.session.open_cursor(uri)
        key = ds.key(18)

        # Detection is off we can successfully change the same key with then without a timestamp.
        self.session.begin_transaction()
        c[key] = ds.value(18)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(21))

        self.session.begin_transaction()
        c[key] = ds.value(19)
        self.session.commit_transaction()
        c.close()

# Test timestamp settings with inconsistent updates.
class test_timestamp26_ts_inconsistent(wttest.WiredTigerTestCase):
    types = [
        ('fix', dict(key_format='r', value_format='8t')),
        ('row', dict(key_format='S', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ]
    assert_or_verbose = [
        ('assert', dict(set_assert=True)),
        ('verbose', dict(set_assert=False)),
    ]
    scenarios = make_scenarios(types, assert_or_verbose)

    def test_timestamp_inconsistent(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        # Create the table with the key consistency checking turned on. That checking will verify
        # any individual key is always or never used with a timestamp. And if it is used with a
        # timestamp that the timestamps are in increasing order for that key.
        uri = 'table:ts'
        config = 'assert' if self.set_assert else 'verbose'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format) +
            ',write_timestamp_usage=ordered,' + config + '=(write_timestamp=on)')

        c = self.session.open_cursor(uri)
        key = ds.key(1)

        # Insert an item at timestamp 2.
        self.session.begin_transaction()
        c[key] = ds.value(1)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(2))

        # Upate the data item at timestamp 1, which should fail.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(1))
        c[key] = ds.value(2)
        msg_ooo ='updates a value with an older timestamp'
        if self.set_assert:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(), '/' + msg_ooo + '/')
        else:
            with self.expectedStderrPattern(msg_ooo):
                    self.session.commit_transaction()

        # Make sure we can successfully add a different key at timestamp 1.
        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(1))
        c[ds.key(2)] = ds.value(3)
        self.session.commit_transaction()
        
        # Insert key1 at timestamp 10 and key2 at 15. Then update both keys in one transaction at
        # timestamp 13. If we're asserting, We should not be allowed to modify the one from 15 and
        # the whole transaction should fail. If we're doing verbose messages, we get a message and
        # the transaction will succeed.
        key1 = ds.key(3)
        key2 = ds.key(4)
        self.session.begin_transaction()
        c[key1] = ds.value(3)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        self.session.begin_transaction()
        c[key2] = ds.value(4)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(15))

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(13))
        c[key1] = ds.value(5)
        c[key2] = ds.value(6)
        if self.set_assert:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(), '/' + msg_ooo + '/')
            self.assertEquals(c[key1], ds.value(3))
            self.assertEquals(c[key2], ds.value(4))
        else:
            with self.expectedStderrPattern(msg_ooo):
                self.session.commit_transaction()
            self.assertEquals(c[key1], ds.value(5))
            self.assertEquals(c[key2], ds.value(6))

        # Separately, we should be able to update key1 at timestamp 10 but not update key2 inserted
        # at timestamp 15.
        if self.set_assert:
            self.session.begin_transaction()
            c[key1] = ds.value(7)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(13))

            self.session.begin_transaction()
            c[key2] = ds.value(8)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(13)), '/' + msg_ooo + '/')
            self.assertEquals(c[key2], ds.value(4))

            # Make sure multiple update attempts still fail and eventually succeed with a later
            # timestamp. This tests that aborted entries in the update chain are not considered
            # for the timestamp check.
            self.session.begin_transaction()
            c[key2] = ds.value(9)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(14)), '/' + msg_ooo + '/')
            self.assertEquals(c[key2], ds.value(4))

            self.session.begin_transaction()
            c[key2] = ds.value(10)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(16))
            self.assertEquals(c[key2], ds.value(10))

    # Try to update a key previously used with timestamps without one. We should get the
    # inconsistent usage error/message.
    def test_timestamp_ts_then_nots(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        # Create the table with the key consistency checking turned on. That checking will verify
        # any individual key is always or never used with a timestamp. And if it is used with a
        # timestamp that the timestamps are in increasing order for that key.
        uri = 'table:ts'
        config = 'assert' if self.set_assert else 'verbose'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format) +
            ',write_timestamp_usage=ordered,' + config + '=(write_timestamp=on)')

        c = self.session.open_cursor(uri)
        key = ds.key(5)

        self.session.begin_transaction()
        c[key] = ds.value(11)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        self.session.begin_transaction()
        c[key] = ds.value(12)
        msg_usage ='configured to always use timestamps once they are first used'
        if self.set_assert:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(), '/' + msg_usage + '/')
            self.assertEquals(c[key], ds.value(11))
        else:
            with self.expectedStderrPattern(msg_usage):
                self.session.commit_transaction()
            self.assertEquals(c[key], ds.value(12))

        self.session.begin_transaction()
        c[key] = ds.value(13)
        if self.set_assert:
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: self.session.commit_transaction(), '/' + msg_usage + '/')
            self.assertEquals(c[key], ds.value(11))
        else:
            self.session.commit_transaction()
            self.assertEquals(c[key], ds.value(13))

    # Smoke test setting the timestamp at various points in the transaction.
    def test_timestamp_ts_order(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        # Create the table with the key consistency checking turned on. That checking will verify
        # any individual key is always or never used with a timestamp. And if it is used with a
        # timestamp that the timestamps are in increasing order for that key.
        uri = 'table:ts'
        config = 'assert' if self.set_assert else 'verbose'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format) +
            ',write_timestamp_usage=ordered,' + config + '=(write_timestamp=on)')

        c = self.session.open_cursor(uri)
        key1 = ds.key(6)
        key2 = ds.key(7)

        self.session.begin_transaction()
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(30))
        c[key1] = ds.value(14)
        c[key2] = ds.value(15)
        self.session.commit_transaction()
        self.assertEquals(c[key1], ds.value(14))
        self.assertEquals(c[key2], ds.value(15))

        self.session.begin_transaction()
        c[key1] = ds.value(16)
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(31))
        c[key2] = ds.value(17)
        self.session.commit_transaction()
        self.assertEquals(c[key1], ds.value(16))
        self.assertEquals(c[key2], ds.value(17))

        self.session.begin_transaction()
        c[key1] = ds.value(18)
        c[key2] = ds.value(19)
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(32))
        self.session.commit_transaction()
        self.assertEquals(c[key1], ds.value(18))
        self.assertEquals(c[key2], ds.value(19))

        self.session.begin_transaction()
        c[key1] = ds.value(20)
        c[key2] = ds.value(21)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(33))
        self.assertEquals(c[key1], ds.value(20))
        self.assertEquals(c[key2], ds.value(21))

# Test that timestamps are ignored in logged files.
class test_timestamp26_log_ts(wttest.WiredTigerTestCase):
    # Turn on logging to cause timestamps to be ignored.
    conn_config = 'log=(enabled=true)'

    types = [
        ('fix', dict(key_format='r', value_format='8t')),
        ('row', dict(key_format='S', value_format='S')),
        ('var', dict(key_format='r', value_format='S')),
    ]
    always = [
        ('always', dict(always=True)),
        ('never', dict(always=False)),
    ]
    scenarios = make_scenarios(types, always)

    # Smoke test that logged files don't complain about timestamps.
    def test_log_ts(self):
        # Create an object that's never written, it's just used to generate valid k/v pairs.
        ds = SimpleDataSet(
            self, 'file:notused', 10, key_format=self.key_format, value_format=self.value_format)

        # Open the object, configuring write_timestamp usage.
        uri = 'table:ts'
        config = ',write_timestamp_usage='
        config += 'always' if self.always else 'never'
        self.session.create(uri,
            'key_format={},value_format={}'.format(self.key_format, self.value_format) +
            config + ',assert=(write_timestamp=on),verbose=(write_timestamp=on)')

        c = self.session.open_cursor(uri)

        # Commit with a timestamp.
        self.session.begin_transaction()
        c[ds.key(1)] = ds.value(1)
        self.session.breakpoint()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Commit without a timestamp.
        self.session.begin_transaction()
        c[ds.key(2)] = ds.value(2)
        self.session.commit_transaction()

if __name__ == '__main__':
    wttest.run()
